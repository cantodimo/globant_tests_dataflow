import logging

import apache_beam as beam
from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io.iobase import RestrictionTracker

from apache_beam.transforms.trigger import AfterCount, Repeatedly

import json
import requests


def call_api( data, url, method, headers, format_function):
    logging.warning( "data received: " + str(data) + " -- url: " + url + " -- method: " + method + " -- headers: " + str(headers) )
    ## data is a json containing the body and another info asociated to the request that we want to 
    ## propagate 

    #url = "https://us-central1-rosy-zoo-390619.cloudfunctions.net/api-rest-dummy"
    #payload = json.dumps(data["body"]) complica la compatibilidad con batch
    payload = format_function(data)
    
    response = requests.request(method, url, headers=headers, data=payload)
    logging.warning( "response.text: " + str(response.text) + " -- type: " + str(type(response.text)) )
    return ( json.loads(response.text), data )

class HttpConnector(beam.PTransform):
    def __init__(self, url, headers, method, batch, format_function= None, iterable=False, max_number_iterations=None, iteration_condition_fn= None):
        if batch < 1:
            raise ValueError('batch size must be >=1')
            
        if format_function is None:
            raise ValueError("""format_function is required, usage:
            def my_format_function(element):
              element can be a single element to make the request, or can be a list of elements,
              
              return {"body": element} by example
              """)
            
        if iterable and max_number_iter is None:
            raise ValueError(" max_number_iterations is required when iterable is True ")
            
        if iterable and iteration_condition_fn is None:
            raise ValueError(""" iteration_condition_fn is required when iterable is True, usage:
            def my_iteration_condition_fn(response):
                if response["more_data_pending"] >0: ## lets suposse that the api says that there is still more data to be downloaded, so we want that the connector try to pull data again
                    return True
                else:
                    return False
            
            """)
            
        self.batch= batch
        self.url= url
        self.method= method
        self.headers= headers
        self.format_function= format_function
        self.iterable = iterable
        self.max_number_iterations = max_number_iterations
        self.iteration_condition_fn= iteration_condition_fn

    def expand(self, pcoll):
        if self.batch == 1:
            ### if is not needed to create a batch, no modification is needed on the input
            input_pcoll= pcoll
        else:
            ## crear global window con aftercount= batch, y discard para no llenar la memoria
            input_pcoll= (
                pcoll | beam.WindowInto(
                    beam.window.GlobalWindows(),
                    trigger=Repeatedly( AfterCount(self.batch) ),
                    accumulation_mode=beam.trigger.AccumulationMode.DISCARDING 
                )
                | beam.Map( lambda x:(0,x) )
                | beam.GroupByKey()
                | beam.Map( lambda x:list(x[1]) )
            )
            
        if self.iterable:
            out_pcoll= input_pcoll | "test pardo" >> beam.ParDo(
                test_splittable_pardo(
                    num_pages_max= self.max_number_iterations,
                    url= self.url,
                    method = self.method,
                    headers = self.headers,  
                    format_function= self.format_function,
                    iteration_condition_fn= self.iteration_condition_fn
                ))
            
        else:
            out_pcoll= input_pcoll | beam.Map(
                call_api,
                self.url,
                self.method,
                self.headers,
                self.format_function
            )

        return out_pcoll
    
    
    
    

##################### test splitteable pardo for http connector

class custom_RestrictionTracker(OffsetRestrictionTracker):
    # creo que toca mirar si el defender reminder es el que modifica el self _chekpointed,
    #  si es asi entonces depronto puedo poner como residual range 0 y_ range todo el intervalo depronto eso lo apaga
    def try_split(self, fraction_of_remainder):
        logging.warning( "inside try_split al inicio, fraction_of_remainder: " + str(fraction_of_remainder) + " -- self._checkpointed: " + str(self._checkpointed) + " -- self._last_claim_attempt: " + str(self._last_claim_attempt) + " -- self._range: " + str(self._range) + " -- residual_range: " +  str(residual_range) ) 
        if not self._checkpointed:
            if self._last_claim_attempt is None:
                cur = self._range.start - 1
            else:
                cur = self._last_claim_attempt
            split_point = cur + 1  
            if split_point <= self._range.stop:
                if fraction_of_remainder == 0:
                    self._checkpointed = True
                self._range, residual_range = self._range.split_at(split_point)
                logging.warning( "inside try_split al final, fraction_of_remainder: " + str(fraction_of_remainder) + " -- self._checkpointed: " + str(self._checkpointed) + " -- self._last_claim_attempt: " + str(self._last_claim_attempt) + " -- self._range: " + str(self._range) + " -- residual_range: " +  str(residual_range) ) 
                #return self._range, residual_range

            # test to see if only when I call the defender_reminder() this condition is triggered 
            # (to try to stop this process when the api says no more pull)
            return OffsetRange(start=1, stop=self._range.stop), OffsetRange(start=self._range.stop, stop=self._range.stop)

    def is_bounded(self):
        return False


class test_splittable_pardo(beam.DoFn, RestrictionProvider):
    def __init__(
        self,
        num_pages_max,
        url,
        method,
        headers,
        format_function,
        iteration_condition_fn
    ):
        self.num_pages_max= num_pages_max
        self.url= url
        self.method = method
        self.headers = headers
        self.format_function= format_function
        self.iteration_condition_fn= iteration_condition_fn
      

    @beam.DoFn.unbounded_per_element()
    def process(self,
                element,
                tracker = beam.DoFn.RestrictionParam(),
                **unused_kwargs):

        restriction = tracker.current_restriction()
        logging.warning( "element received " + str(element) + "-----" + str(restriction.start) + " -- " + str(restriction.stop))        
        for position in range(restriction.start, restriction.stop + 1):
            if tracker.try_claim(position):
                response= call_api(
                    element,
                    self.url,
                    self.method, 
                    self.headers, 
                    self.format_function
                )
                if iteration_condition_fn(response[0]):
                    logging.warning( "continue polling, response: " + str(response) + " -- position: " + str(position))
                    yield (response[0], response[1], position)

                else:
                    logging.warning( "last response api for response: " + str(response) + " -- position: " + str(position))
                    
                    # said that was the last message
                    tracker.defer_remainder()
                    yield (response[0], response[1], -1000) # flag to check on bigquery if the pardo stops only when the api

                
            else:
                logging.warning( "not claim on element (finish element), response: " + str(response) + " -- position: " + str(position) ) 
                return

    def create_tracker(self, restriction: OffsetRange) -> RestrictionTracker:
        #out= OffsetRestrictionTracker(restriction)
        out= custom_RestrictionTracker(restriction)
        logging.warning( "inside create tracker, restriction: " + str(restriction) + " -- returned: " + str(out) )
        return out

    def initial_restriction(self, element) -> OffsetRange:
        out= OffsetRange(start=1, stop=self.num_pages_max)
        logging.warning( "inside initial restriction, element: " + str(element) + " -- returned: " + str(out) )
        return out

    def restriction_size(self, element, restriction: OffsetRange):

        ### trying to stop the process of the element sending a size of 0 when this function is
        ### triggered and its current size is less that the initial, IT DOESNT WORK...
        #if restriction.size() < (self.num_pages_max -1):
        #    out= 0
        #else:
        #    out=  restriction.size()

        out=  restriction.size()
        logging.warning( "inside restriction_size, element: " + str(element) + " -- restriction: " + str(restriction) + " -- returned: " + str(out) )
        return out