#!/bin/bash

# get total number of blocks on the current active blockchain
BLOCKCOUNT=$(bitcoin-cli getblockcount)

# specify optional delay parameter (trailing behind leader block)
DELAY=0

# specify starting block (0 for genesis block)
START=0

# specify ending block (current block with any trailing delay)
END=$((BLOCKCOUNT-DELAY))
echo $END

# loop through all blocks and run JSON-RPC calls to write to S3
for ((i=START;i<=END;i++))

do

  echo "Sending block $i"

  # set block height
  BLOCKHEIGHT=$i

  # get block hash
  BLOCKHASH=$(bitcoin-cli getblockhash $BLOCKHEIGHT)
  echo $BLOCKHASH

  # set block file name
  FILENAME="block"${i}".json"
  #echo $FILENAME

  # get block JSON data and write to aws s3 service
  bitcoin-cli getblock ${BLOCKHASH} 2 | aws s3 cp - s3://bitcoin-json-mycelias/${FILENAME}

done
