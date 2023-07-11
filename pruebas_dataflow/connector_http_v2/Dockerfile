# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM gcr.io/dataflow-templates-base/python3-template-launcher-base
#FROM gcr.io/dataflow-templates-base/java8-template-launcher-base

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"

#COPY . /template

# We could get rid of installing libffi-dev and git, or we could leave them.
#RUN apt-get update 
#RUN apt-get install software-properties-common -y
#RUN add-apt-repository ppa:webupd8team/java
#RUN echo "0"
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2
RUN apt-get update && \
apt-get install -y --no-install-recommends \
        openjdk-11-jre

# Prints installed java version, just for checking
RUN java --version

RUN apt-get update 
#RUN echo "1"
RUN apt-get install -y libffi-dev git
#RUN echo "2"
RUN rm -rf /var/lib/apt/lists/*
#RUN echo "3"
# Upgrade pip and install the requirements.
RUN  pip install --no-cache-dir --upgrade pip
#RUN echo "4"

RUN pip install --no-cache-dir apache-beam[gcp]==2.41.0
RUN pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache apache-beam[gcp]==2.41.0

RUN apt-get clean

COPY . /template

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