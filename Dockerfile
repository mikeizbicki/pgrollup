ARG BASE_IMAGE_VERSION=latest

FROM postgres:$BASE_IMAGE_VERSION

# install system packages
COPY ./install_dependencies.sh /tmp
RUN export PG_MAJOR=`apt list --installed 2>&1 | sed -n "s/^postgresql-\([0-9.]*\)\/.*/\1/p"`             \
 && export PG_MINOR=`apt list --installed 2>&1 | sed -n "s/^postgresql-$PG_MAJOR\/\S*\s\(\S*\)\s.*/\1/p"` \
 && apt-get update \
 && apt-get install -y --no-install-recommends --allow-downgrades \
        autoconf \
        gcc \
        g++ \
        git \
        make \
        postgresql-server-dev-$PG_MAJOR \
        postgresql-plpython3-$PG_MAJOR \
        python3 \
        python3-pip \
        wget \
        zip \
        unzip \
 && sh /tmp/install_dependencies.sh \
 && apt-get purge --auto-remove -y \
        autoconf \
        gcc \
        g++ \
        git \
        make \
        postgresql-server-dev-$PG_MAJOR \
        postgresql-plpython3-$PG_MAJOR \
        python3 \
        python3-pip \
        wget \
        zip \
        unzip \
 && apt-get autoremove

# create a tablespace directory for the testcases
RUN mkdir /tmp/tablespace \
 && chown postgres /tmp/tablespace

WORKDIR /tmp/pg_rollup

RUN apt-get install -y --no-install-recommends \
        postgresql-plpython3-$BASE_IMAGE_VERSION \
        python3 \
        python3-pip

# copy over the project
COPY . /tmp/pg_rollup
RUN pip3 install .
RUN make USE_PGXS=1 \
 && make USE_PGXS=1 install
