#!/bin/sh

BADUA_GROUP_ID="br.usp.each.saeg"
BADUA_VERSION="0.6.0"
BADUA_AGENT_RT="./dfc/ba-dua-agent-rt-0.6.0-all.jar"
BADUA_CLI="./dfc/ba-dua-cli-0.6.0-all.jar"

mvn install:install-file \
        -DgroupId=${BADUA_GROUP_ID} \
        -DartifactId=ba-dua-agent-rt \
        -Dversion=${BADUA_VERSION} \
        -Dclassifier=all \
        -Dpackaging=jar \
        -Dfile=${BADUA_AGENT_RT}

mvn install:install-file \
        -DgroupId=${BADUA_GROUP_ID} \
        -DartifactId=ba-dua-cli \
        -Dversion=${BADUA_VERSION} \
        -Dpackaging=jar \
        -Dfile=${BADUA_CLI}
