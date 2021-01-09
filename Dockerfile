FROM postgres:12

RUN apt-get update && apt-get install -y \
    postgresql-server-dev-12 \
    postgresql-plpython3-12 \
    python3 \
    python3-pip

WORKDIR /tmp/pg_rollup

# copy over the project
COPY . /tmp/pg_rollup
RUN pip3 install .
RUN make USE_PGXS=1 \
 && make USE_PGXS=1 install
