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
        libboost-dev \
        make \
        postgresql-server-dev-$PG_MAJOR \
        postgresql-plpython3-$PG_MAJOR \
        python3 \
        python3-pip \
        python3-setuptools \
        wget \
        zip \
        unzip \
 && sh /tmp/install_dependencies.sh \
 && apt-get purge --auto-remove -y \
        autoconf \
        gcc \
        g++ \
        git \
        libboost-dev \
        make \
        postgresql-server-dev-$PG_MAJOR \
        postgresql-plpython3-$PG_MAJOR \
        python3 \
        python3-pip \
        python3-setuptools \
        wget \
        zip \
        unzip \
 && apt-get autoremove

# install vector
# FIXME:
# this is done as a separate step from the other dependencies because this library is getting lots of development
# and this minimizes build times;
# the downside is that the dependencies will not get cleaned up from the resulting docker image
RUN export PG_MAJOR=`apt list --installed 2>&1 | sed -n "s/^postgresql-\([0-9.]*\)\/.*/\1/p"`             \
 && export PG_MINOR=`apt list --installed 2>&1 | sed -n "s/^postgresql-$PG_MAJOR\/\S*\s\(\S*\)\s.*/\1/p"` \
 && apt-get update \
 && apt-get install -y --no-install-recommends --allow-downgrades \
        gcc \
        git \
        make \
        postgresql-server-dev-$PG_MAJOR \
        python3 \
        python3-pip \
        python3-setuptools
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/pgvector \
 && cd pgvector \
 && make -j \
 && make install \
 && rm -rf /tmp/pgvector

# create a tablespace directory for the testcases
RUN mkdir /tmp/tablespace \
 && chown postgres /tmp/tablespace

WORKDIR /tmp/pg_rollup

RUN export PG_MAJOR=`apt list --installed 2>&1 | sed -n "s/^postgresql-\([0-9.]*\)\/.*/\1/p"`             \
 && export PG_MINOR=`apt list --installed 2>&1 | sed -n "s/^postgresql-$PG_MAJOR\/\S*\s\(\S*\)\s.*/\1/p"` \
 && apt-get install -y --no-install-recommends \
        postgresql-plpython3-$PG_MAJOR \
        python3 \
        python3-pip \
        python3-setuptools \
        make

RUN export PG_MAJOR=`apt list --installed 2>&1 | sed -n "s/^postgresql-\([0-9.]*\)\/.*/\1/p"`             \
 && export PG_MINOR=`apt list --installed 2>&1 | sed -n "s/^postgresql-$PG_MAJOR\/\S*\s\(\S*\)\s.*/\1/p"` \
 && apt-get install -y --no-install-recommends \
        postgresql-server-dev-$PG_MAJOR \
        gcc

# install citus
# see: http://docs.citusdata.com/en/v10.0/installation/multi_node_debian.html#steps-to-be-executed-on-all-nodes
RUN apt-get install -y curl \
 && curl https://install.citusdata.com/community/deb.sh | bash \
 && apt-get -y install postgresql-$PG_MAJOR-citus-10.0

# copy over the project
COPY . /tmp/pg_rollup
COPY postgresql.conf /etc/postgresql.conf.pg_rollup
RUN pip3 install -r requirements.txt && pip3 install .
RUN make USE_PGXS=1 \
 && make USE_PGXS=1 install
