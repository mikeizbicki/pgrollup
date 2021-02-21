FROM postgres:12

# install system packages
RUN apt-get update && apt-get install -y \
    autoconf \
    gcc \
    git \
    make \
    postgresql-server-dev-12 \
    postgresql-plpython3-12 \
    python3 \
    python3-pip

# install postgres hll extension from source
RUN cd /tmp \
 && git clone https://github.com/citusdata/postgresql-hll \
 && cd postgresql-hll \
 && git checkout v2.15 \
 && make \
 && make install \
 && rm -rf /tmp/postgresql-hll

# install the tdigest plugin from source
RUN cd /tmp \
 && git clone https://github.com/tvondra/tdigest/ \
 && cd tdigest \
 && git checkout v1.0.1 \
 && make \
 && make install \
 && rm -rf /tmp/tdigest

# install the cms_topn plugin from source
#RUN cd /tmp \
 #&& git clone https://github.com/ozturkosu/cms_topn \
 #&& cd cms_topn \
 #&& git checkout 78ce0d1e0437c0b35419d963685d5de57a87078e \
 #&& make \
 #&& make install \
 #&& rm -rf /tmp/cms_topn

# install pg_cron plugin from source
RUN cd /tmp \
 && git clone https://github.com/citusdata/pg_cron.git \
 && cd pg_cron \
 && git checkout v1.3.0 \
 && make \
 && make install \
 && rm -rf /tmp/pg_cron
COPY ./postgresql.conf /etc/postgresql.conf

# create a tablespace directory for the testcases
RUN mkdir /tmp/tablespace \
 && chown postgres /tmp/tablespace

WORKDIR /tmp/pg_rollup

# copy over the project
COPY . /tmp/pg_rollup
RUN pip3 install .
RUN make USE_PGXS=1 \
 && make USE_PGXS=1 install
