# NdBench

[![Build Status](https://travis-ci.org/Netflix/ndbench.svg)](https://travis-ci.org/Netflix/ndbench)
[![Dev chat at https://gitter.im/Netflix/ndbench](https://badges.gitter.im/Netflix/ndbench.svg)](https://gitter.im/Netflix/ndbench?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


**About NdBench**

Details about the features can be found in the [Wiki](https://github.com/Netflix/ndbench/wiki)

## Workflow

The stable version of NdBench is the [master]( https://github.com/Netflix/ndbench/tree/master ) branch.

For questions or contributions, please consider reading [CONTRIBUTING.md](CONTRIBUTING.md).

## Build

NdBench comes with a Gradle wrapper

    ./gradlew build

The gradlew script will pull down all necessary gradle components/infrastructure automatically, then run the build.

NdBench provides several default implementations (AWS, Configuration, credentials etc). You can use these or choose to create your own. NdBench is currently working on AWS and your local environment. We are open to contributions to support other platforms as well.

## Howto



## Run


## Configuration

You can provide properties by using ndbench{version}.jar in your web container and then implementing [IConfiguration Interface](https://github.com/Netflix/ndbench/blob/master/dynomitemanager/src/main/java/com/netflix/dynomitemanager/sidecore/IConfiguration.java). More details on the how the configuration can be found in the [Wiki](https://github.com/Netflix/ndbench/wiki/Configuration).

## Help

Need some help with either getting up and going or some problems with the code?

   * Submit an issue to repo
   * Chat with us on [![Dev chat at https://gitter.im/Netflix/dynomite](https://badges.gitter.im/Netflix/dynomite.svg)](https://gitter.im/Netflix/dynomite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
