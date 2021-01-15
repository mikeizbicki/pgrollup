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

WORKDIR /tmp/pg_rollup

# copy over the project
COPY . /tmp/pg_rollup
RUN pip3 install .
RUN make USE_PGXS=1 \
 && make USE_PGXS=1 install
