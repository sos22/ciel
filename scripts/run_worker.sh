#!/bin/bash

if [ "${0#/}" = "${0}" ]
then
    me=$(pwd)/$0
else
    me=$0
fi

install_prefix=${me%bin/run_worker.sh}
if [ "$install_prefix" = "$me" ]
then
    BASE=$(pwd)
    my_python_path="$BASE/src/python"
    sw_worker=${BASE}/scripts/sw-worker
    staticbase=${BASE}/src/js/skyweb/
    lighttpd_conf=${BASE}/src/python/skywriting/runtime/lighttpd.conf
    store_base=${BASE}
else
    if [ "$install_prefix" != "/" ] && [ "$install_prefix" != "/usr" ] && [ "$install_prefix" != "/usr/local" ]
    then
	PYTHONVER=$(python --version 2>&1 | cut -d' ' -f 2 | cut -d '.' -f1,2)
	my_python_path=$install_prefix/lib/python${PYTHONVER}/site-packages
    else
	my_python_path=""
    fi
    sw_worker=${install_prefix}/bin/sw-worker
    staticbase=${install_prefix}/share/ciel/skyweb/
    lighttpd_conf=${install_prefix}/share/ciel/lighttpd.conf
    store_base=${install_prefix}/var/run/ciel
fi
if ! [ -z "$my_python_path" ]
then
    if [ -z "$PYTHONPATH" ]
    then
	export PYTHONPATH=$my_python_path
    else
	PYTHONPATH=${PYTHONPATH}:$my_python_path
    fi
fi

if [[ $REL_BLOCK_LOCATION == "" ]]; then
    REL_BLOCK_LOCATION="store/"
fi
ABS_BLOCK_LOCATION="${store_base}/$REL_BLOCK_LOCATION"

MASTER=${MASTER_HOST:-http://127.0.0.1:8000}

WORKER_PORT=${WORKER_PORT:-8001}

if [[ $SCALA_HOME == "" ]]; then
    if [ -e /opt/skywriting/ext/scala-2.9.0.1 ]; then
	SCALA_HOME=/opt/skywriting/ext/scala-2.9.0.1
    fi
fi

if [[ $SCALA_HOME != "" ]]; then
    SCALA_CLASSPATH=$SCALA_HOME/lib/scala-library.jar
    if [ ! -e "${SCALA_CLASSPATH}" ]; then
      echo Not found: ${SCALA_CLASSPATH}
      exit 1
    fi
fi

LIGHTTPD_BIN=`which lighttpd`
if [ "$LIGHTTPD_BIN" != "" ]; then
  EXTRA_CONF="${EXTRA_CONF} --lighttpd-conf ${lighttpd_conf}"
fi

GSON_VERSION=1.7.1
export CLASSPATH=${BASE}/dist/skywriting.jar:${BASE}/ext/google-gson-${GSON_VERSION}/gson-${GSON_VERSION}.jar:${SCALA_CLASSPATH}
export SW_MONO_LOADER_PATH=${BASE}/src/csharp/bin/loader.exe
export SW_C_LOADER_PATH=${BASE}/src/c/src/loader
export CIEL_SKYPY_BASE=${BASE}/src/python/skywriting/runtime/worker/skypy
export CIEL_SW_BASE=${BASE}/src/python/skywriting/lang
export CIEL_SW_STDLIB=${BASE}/src/sw/stdlib
${sw_worker} --role worker --master ${MASTER} --port $WORKER_PORT --staticbase "$staticbase" ${HTTPD} -b $ABS_BLOCK_LOCATION -T ciel-process-aaca0f5eb4d2d98a6ce6dffa99f8254b ${EXTRA_CONF} $*
