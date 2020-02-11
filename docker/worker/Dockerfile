ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
FROM puckel/docker-airflow:1.10.4

# Install git
USER root
RUN apt-get update -y && \
  apt-get install -y git libssl-dev libreadline-dev jq


# Install rbenv and ruby for airflow user.
USER airflow
ARG AIRFLOW_USER_HOME=/usr/local/airflow
RUN git clone https://github.com/rbenv/rbenv.git ${AIRFLOW_USER_HOME}/.rbenv
RUN git clone https://github.com/rbenv/ruby-build.git ${AIRFLOW_USER_HOME}/.rbenv/plugins/ruby-build
ENV PATH ${AIRFLOW_USER_HOME}/.rbenv/shims:${AIRFLOW_USER_HOME}/.rbenv/bin:$PATH
ENV RBENV_SHELL=bash
ENV CONFIGURE_OPTS --disable-install-doc

RUN rbenv install 2.6.4
ENV RBENV_VERSION 2.6.4
RUN gem install bundler

# start airflow worker
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
