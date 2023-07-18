FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2
RUN apt-get update && \
apt-get install -y --no-install-recommends \
        openjdk-11-jre

# Prints installed java version, just for checking
RUN java --version

RUN apt-get update
RUN apt-get install -y libffi-dev git
RUN rm -rf /var/lib/apt/lists/*
RUN  pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir apache-beam[gcp]==2.45.0
RUN pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache apache-beam[gcp]==2.45.0

RUN apt-get clean

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/itd-saptm-apachebeam/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/itd-saptm-apachebeam/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/itd-saptm-apachebeam/pipelines/itd-kf-kf-filtering.py"

COPY . /itd-saptm-apachebeam/

#RUN pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE lo hace una y otra ves cuando se redespliega, lo quito pa ahorrar tiempo
# Download the requirements to speed up launching the Dataflow job.
#RUN echo "5"
#RUN pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE
#RUN echo "6"

#RUN echo 'deb http://ftp.debian.org/debian stretch-backports main' | tee /etc/apt/sources.list.d/stretch-backports.list
#RUN apt-get install -y openjdk-11-jre-headless
#RUN echo "7"
#RUN apt-get clean

# Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]