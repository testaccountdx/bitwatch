# BitWatch

![Image of Cover](images/cover.png)

Enabling cyber forensics on the Bitcoin blockchain by linking addresses used by the same individual and/or entity.

Results can inform criminal investigations into money flows for dark webs (i.e., Silk Road) and fraud detection by tracing network transaction patterns.

Current implementation is for Bitcoin, but solution is highly transferable to other public blockchains (i.e., Ethereum, Bitcoin Cash, Litecoin).

# Table of Contents
1. [Motivation](README.md#Motivation)
2. [Dataset](README.md#Dataset)
3. [Methodology](README.md#Methodology)
4. [Architecture](README.md#Architecture)
5. [Installation](README.md#Installation)
6. [Web App](README.md#Web-App)

## Motivation

$350 Billion USD - that number represents the current value of the cryptocurrency market (July-2019).
Despite being highly lucrative, the crypto market is poorly understood.
Specifically, forensics agencies are deeply interested in certain types of transaction behavior such as money laundering, price manipulation, and international remittances.

BitWatch enables cyber forensics on the Bitcoin blockchain by linking addresses used by the same individual / entity. Results can inform fraud detection and criminal investigations into money flows for dark webs (i.e., Silk Road).


## Dataset

Blockchain data reflects the historical Bitcoin blockchain up to block ~580,000 (Jun-2019).

Data was acquired by running a full Bitcoin Core node and deserializing block data (blk*.dat files) into JSON format using JSON-RPC.

Detailed instructions for setting up a full node with transaction indexing can be found [here](https://www.buildblockchain.tech/blog/btc-node-developers-guide).


## Methodology

Bitcoin users can hold multiple addresses - creating a new address is straightforward, low-cost, and nearly instantaneous.
These addresses can then be used throughout a user's transaction history to accumulate and sell Bitcoin.

Assuming a forensics agent (i.e., FBI, CIA) gains access to a single address for a given individual under investigation, BitWatch can return all addresses (address cluster) likely to be associated with that user.

![Image of Method1](images/method1.png)


BitWatch exploits an innate property of Bitcoin transactions called the 'multi-input heuristic' to perform address clustering.
For example, let's say you walk into your favorite coffee shop and buy a rather expensive latte for $5.36 USD. You take a five dollar bill, quarter, dime, and penny out of your wallet / coin purse.
In this case we know that all those bills and coins, the 'transaction inputs', came from the same individual - you.
This heuristic can be applied to Bitcoin transactions - all input transactions (UTXOs) are likely to come from the same individual.

Disclaimer: this heuristic does not hold for CoinJoin transactions where groups of individuals can collude and mix inputs into a given transaction.
However, not all Bitcoin wallets support the CoinJoin feature and most users do not commonly use CoinJoin.
In addition, algorithms exist to detect CoinJoin transactions, which may be implemented in a future release.

Fun side note: the privacy implications of the 'multi-input heuristic' have a far-reaching history.
This property was mentioned in the original [Bitcoin whitepaper](https://bitcoin.org/bitcoin.pdf) in Section 10 by Satoshi Nakamoto, the founder of Bitcoin.

![Image of Method1](images/method2.png)


Once we have the set of addresses at the individual transaction level, we can apply the classic Disjoint Set (a.k.a. Union Find) graph algorithm to generate address clusters that likely belong to the same individual / entity.
Extending the coffee transaction example from before, we have a set of 4 unique input addresses associated with the coffee transaction.

Now add another transaction called the dinner transaction, which has 2 unique input addresses.
In the diagram below, we can see that address #2 (blue) is present in both the coffee set and the dinner set.
Therefore, we can assume that the same individual holds all five unique addresses.

![Image of Method1](images/method3.png)

Applying the Disjoint Set algorithm using a relational style model is not efficient as it requires many self joins.
Instead, we use a graph model with vertices representing unique address IDs and edges representing connections between addresses at the individual transaction level.
We leverage the [connectedComponents()](https://docs.databricks.com/spark/latest/graph-analysis/graphframes/user-guide-scala.html#connected-components) method in Spark Graphframes as an efficient implementation of Disjoint Set.


## Architecture

![Image of Pipeline](images/pipelinefinal.png)

### Data Acquisition

Data is acquired by running JSON-RPC calls from a full Bitcoin Core node.

Run `./json-rpc-pase-all-blocks.sh` in `/src/bash` directory to deserialize Bitcoin block data into JSON and write into dedicated AWS S3 bucket.
This must be run from a full Bitcoin Core node with transaction indexing enabled (see [here](https://www.buildblockchain.tech/blog/btc-node-developers-guide) for setup instructions)


### Ingestion

BitWatch runs on top of a Spark cluster (one c5.large for master, three c5.2xlarge for workers) and a single PostgreSQL instance (one m4.large).

Data is ingested with Spark from an S3 bucket that holds JSON files (one file for each blockchain block).

Results are then written out to PostgreSQL in a tabular format in a `transactions` table (each row represents one transaction).

(See Installation section below) Run `process-json.py` in `src/spark` directory using `spark-submit` command in PySpark to ingest JSON files from AWS S3 bucket.


### Compute

BitWatch uses Spark GraphFrames (built on top of a vertex DataFrame and edge DataFrame) to run `.connectedComponents()` method for generating address clusters.

`.connectedComponents()` is an implementation of the Disjoint Set (a.k.a. Union Find) algorithm to cluster addresses across Bitcoin transactions.

Using a graph model for processing transaction data is crucial as Disjoint Set on a relational model is much slower compared to a graph model.

(See Installation section below) run `tx-lookup-cluster.py` in `src/spark` directory using `spark-submit` command in PySpark to process `transactions` table in PostgreSQL and generate address clusters.


## Installation

Installation (i.e., Bitcoin Core, Spark, PostgreSQL) will occur on on AWS EC2 instances.

Before going through the below instructions, please familiarize yourself with setting up security groups in AWS [here](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html).


### Bitcoin Core

We must first set up a [Bitcoin Core](https://github.com/bitcoin/bitcoin) full node to synchronize block data with the main Bitcoin network.
This will allow access to the full history of the Bitcoin blockchain and allow for local (i.e., trustless) validation of all transactions.
Local validation is required to avoid tainted data (i.e., just downloading the entire blockchain from an online .zip file).
Setting up a Bitcoin Core node is required in order to access block data via simple JSON-RPC calls.

Most guides for setting up a full node are geared towards setup on local machines, but we will install Bitcoin Core on an AWS EC2 instance.
Launch an EC2 instance using the **Ubuntu Server 18.04 LTS (HVM), SSD Volume Type** m4.large image type and set root volume storage to 500 GB.
Then [SSH into the instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) and run the following commands:

    # run following comamnds to install Bitcoin Core using PPA
	sudo apt-add-repository ppa:bitcoin/bitcoin
    sudo apt-get update
    sudo apt-get install bitcoind
    
    # run bitcoind -version to check bitcoin software version
    bitcoind -version
    
You now have the latest Bitcoin Core software, but you are missing 10+ years of Bitcoin transaction history.
Given this is a full node, it stores and validates the full history of the blockchain.
The download process takes a long time and requires ~350 GB disk space (as of Jul-2019).
An AWS EC2 instance should be able to download the full blockchain in ~1-2 days.
	
    # start downloading blockchain with transaction indexing turned on
	bitcoind -daemon -txindex=1
	
	# check block download progress
	cd ~/.bitcoin/d
	ls
	tail -f debug.log
	
When the node performs the initial sync, log files will fly by very quickly.
Hit Ctrl + C to exit tailing the file and examine the messages.
Bitcoin-CLI commands can now be used to interface with JSON-RPC and return data in JSON format:

    # stop and restart Bitcoin Core
    bitcoin-cli stop
    bitcoind -daemon
    
    # view number of blocks in longest blockchain
    bitcoin-cli getblock

We will now launch a bash script (`json-rpc-parse-all-blocks.sh` in the `src/bash` directory) that leverages JSON-RPC to write each block's transaction data in JSON format into a pre-specified AWS S3 bucket.
See simple instructions for setting up an AWS S3 bucket [here](https://aws.amazon.com/s3/getting-started/).
Deserializing block data (blk*.dat files) using JSON-RPC results in ~1.8 TB of JSON data stored in S3.
Note this script must be copied into the Bitcoin Core EC2 instance and run within that instance.

See instructions for secure copy (scp) [here](https://linuxize.com/post/how-to-use-scp-command-to-securely-transfer-files/).

    # launch bash script to write entire block history into S3 in JSON format (one file per Bitcoin block)
    chmod +x json-rpc-parse-all-blocks.sh
    ./json-rpc-parse-all-blocks.sh

More comprehensive instructions for tinkering with Bitcoin Core and JSON-RPC can be found [here](https://www.buildblockchain.tech/blog/btc-node-developers-guide):


### PostgreSQL

We will use a dedicated PostgreSQL instance to store data processed in Spark (both ETL and Compute aspect of pipeline).

We will launch an EC2 instance using the **Ubuntu Server 18.04 LTS (HVM), SSD Volume Type** m4.large image type and set root volume storage to 1 TB.
Then [SSH into the instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) and run the following commands:

    # run update and install PostgreSQL
	sudo apt update
	sudo apt install postgresql postgresql-contrib
	
	# start PostgreSQL and check status
	sudo systemctl start postgresql
	sudo systemctl status postgresql
	
	# log in as default postgres user
	sudo -u postgres -i
	
	# change default postgres user password
	psql ALTER USER postgres PASSWORD 'myPassword';
	
All configuration files are stored here:

    /etc/postgresql/10/main
    
The `postgresql.conf` and `pga_hba.conf` files are the most relevant for configuring a database.

    #postgresql.conf
    #listen_addresses = 'localhost' # what IP address(es) to listen on;
                                    # comma-separated list of addresses;
                                    # defaults to 'localhost'; 
                                    # use '*' for all
                                    # (change requires restart)
    port = 5432                     # (change requires restart)
    max_connections = 100           # (change requires restart)
    ...

Change `listen_addresses` to `*` instead of `localhost` and restart postgres service using `sudo systemctl restart postgresql`.

The `pga_hba.conf` file controls which hosts are allows to connect.
Add the following line under the IPv4 section in that file:

    host    <database>      <user>       0.0.0.0/0        md5

The PostgreSQL instance should now be setup and ready for remote connection.


### Spark

We will set up Spark on a cluster of EC2 instances to run the scripts for processing JSON data from the S3 bucket to PostgreSQL (`process-json.py`) and computing address clusters (`tx-lookup-cluster.py`).

We will launch four EC2 instances, each using the **Ubuntu Server 18.04 LTS (HVM), SSD Volume Type** 1 x m5.large (master), 3x m5.2xlarge (workers) image type and set root volume storage to 200 GB.
Then [SSH into the instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) and run the following commands:

    # run update and install java and scala
	sudo apt update
	sudo apt install openjdk-8-jre -y
	sudo apt install scala -y
	
	# install sbt
    echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
    sudo apt-get update
    sudo apt-get install sbt
    
    # install Spark 2.4.3
    wget http://apache.mirrors.tds.net/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz -P ~/Downloads
    sudo tar zxvf ~/Downloads/spark-2.4.3-bin-hadoop2.7.tgz -C /usr/local
    sudo mv /usr/local/spark-2.4.3-bin-hadoop2.7 /usr/local/spark
    sudo chown -R ubuntu /usr/local/spark
    
    # edit ~/.profile
    nano ~/.profile
    
    # add in following lines to ~/.profile
    export SPARK_HOME=/usr/local/spark
    export PATH=$PATH:$SPARK_HOME/bin
    export PYSPARK_PYTHON=python3
    
    # update environment variables
    source ~/.profile
    
    # configure Spark
    cp /usr/local/spark/conf/spark-env.sh.template /usr/local/spark/conf/spark-env.sh
    
    # edit spark-env.sh
    nano /usr/local/spark/conf/spark-env.sh
    
    # add in following lines to spark-env.sh
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    export SPARK_PUBLIC_DNS="<MASTER-public-dns>" (i.e., like ec2-x-xx-xxxx-xx.compute1.amazonaws.com)

Go to master node and run:

    touch $SPARK_HOME/conf/slaves
    
Add each worker to the file by opening $SPARK_HOME/conf/slaves and copying the public DNS (one per line):

    nano $SPARK_HOME/conf/slaves
    
Connect master node to worker nodes (commands are only for master node):

    sudo apt install openssh-server openssh-client
    cd ~/.ssh
    ssh-keygen -t rsa -P ""
    
    Generating public/private rsa key pair.
    Enter file in which to save the key: id_rsa
    Your identification has been saved in id_rsa.
    Your public key has been saved in id_rsa.pub.
    
Manually copy id_rsa.pub key top worker nodes:

    # on master:
    cat ~/.ssh/id_rsa.pub
    
    # on slaves:
    vi ~/.ssh/authorized_keys
    # paste the key
    
    # test connection from master to workers
    ssh ubuntu@ec2.x--x-x-x-x-x
    
Start Spark server (should see Spark ASCII art):

    # start Spark server
    sh /usr/local/spark/sbin/start-all.sh
    
Check everything is working by going to the master_public_ip:8080 (requires port 8080 open).
If you see the SparkUI with your workers up, Spark setup is complete.

Now we will install PySpark

    # install pip3
    sudo apt install python3-pip -y
    
    # install pyspark and findspark
    pip3 install pyspark
    pip3 install findspark --user
    
We should now be able to submit PySpark scripts (i.e., `process-json.py` and `tx-lookup-cluster.py`) files using `spark-submit` and run in Spark.


## Web App

BitWatch has a simple web interface for address cluster lookup based on a single input address.

Website: [BitWatch](https://www.mycelias.com)