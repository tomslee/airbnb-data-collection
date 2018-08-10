# Run script(s) with Docker

1. Install Docker from [here](https://docs.docker.com/install/)
2. Install Docker Compose from [here](https://docs.docker.com/compose/install/)
3. Start the containers from root of the cloned project: 

   ```console
   cd docker
   docker-compose up
   ```
4. To get a shell on the collector container:

   ```console
   docker exec -it airbnbcollector /bin/bash
   ```
   The python scripts from the projects are located in /collector
   There is a config ready for the execution under /collector/configs

   To get a shell on the db container:

   ```shell
   docker exec -it airbnbcollector /bin/bash
   ```
   In this shell you can query the database. The database is initialized on container startup.

   For the scripts you can run, please check [main documentation](../README.md)
