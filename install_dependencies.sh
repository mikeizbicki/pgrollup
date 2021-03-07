
# stop and throw an error on error
set -e

# install postgres hll extension from source
cd /tmp 
git clone https://github.com/citusdata/postgresql-hll
cd postgresql-hll
git checkout v2.15
make
make install
rm -rf /tmp/postgresql-hll

# install the topn plugin from source
cd /tmp
git clone https://github.com/citusdata/postgresql-topn
cd postgresql-topn
git checkout v2.3.1
make
make install
rm -rf /tmp/postgresql-topn

# install the tdigest plugin from source
cd /tmp
git clone https://github.com/tvondra/tdigest/
cd tdigest
git checkout v1.0.1
make
make install
rm -rf /tmp/tdigest

# install datasketches
cd /tmp
wget http://api.pgxn.org/dist/datasketches/1.3.0/datasketches-1.3.0.zip
unzip datasketches-1.3.0.zip
cd datasketches-1.3.0
make
(make install || true) # FIXME: this install step throws an error, but seems to install everything correctly
rm -rf /tmp/datasketches-1.3.0

# install pg_cron plugin from source
cd /tmp
git clone https://github.com/citusdata/pg_cron.git
cd pg_cron
git checkout v1.3.0
make
make install
rm -rf /tmp/pg_cron

# install the cms_topn plugin from source
#RUN cd /tmp \
 #&& git clone https://github.com/ozturkosu/cms_topn \
 #&& cd cms_topn \
 #&& git checkout 78ce0d1e0437c0b35419d963685d5de57a87078e \
 #&& make \
 #&& make install \
 #&& rm -rf /tmp/cms_topn
