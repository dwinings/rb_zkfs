ZKFS
======

Access zookeeper from the comfort of your filesystem!

##Usage:
`bundle exec ruby zkfs.rb localhost:2181/chroot ~/zkmount`

Note that instead of killing the script, you should simply unmount the
directory you mounted it to. Also, in order to run this, you will need to have
fuse installed. For mac, this means that you should do `brew install osxfuse`.
For linux you can likely just install the required header packages for fuse
supplied by the OS. (Probably something like libfuse-dev)
