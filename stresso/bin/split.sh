#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
. $BIN_DIR/load-env.sh

$FLUO_CMD exec $FLUO_APP_NAME stresso.trie.Split $FLUO_CONN $FLUO_APP_NAME "$TABLE_PROPS" $@
