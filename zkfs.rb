require 'fusefs'
require 'zk'
require 'json'
require 'pathname'
require 'andand'

# RFUSEFS Installation instructions on mac:
#
# brew install osxfuse
# git clone git@github.com:winebarrel/rfuse.git
# cd rfuse
# git checkout fix_for_osx
# bundle install
# bundle exec rake install
# gem install rfusefs

# USAGE: ruby zkfuse.rb localhost:2181/chroot ./zkmount

# rubocop:disable all
# Monkey patch for rfusefs to get it to (correctly) return no xattrs on mounted directory
module FuseFS
  class Fuse::Root
    def getxattr(ctx, path, name)
      ''
    end

    def listxattr(ctx, path)
      ''
    end

    def setxattr(ctx, path, name, value, flags)
      value
    end

    def getattr(ctx,path)

      return wrap_context(ctx,__method__,path) if ctx

      uid = Process.uid
      gid = Process.gid

      if  path == "/" || @root.directory?(path)
        #set "w" flag based on can_mkdir? || can_write? to path + "/._rfuse_check"
        write_test_path = (path == "/" ? "" : path) + CHECK_FILE
        #                                                            My patch...          0777 -> 0755
        mode = (@root.can_mkdir?(write_test_path) || @root.can_write?(write_test_path)) ? 0755 : 0555
        atime,mtime,ctime = @root.times(path)
        #nlink is set to 1 because apparently this makes find work.
        return RFuse::Stat.directory(mode,{ :uid => uid, :gid => gid, :nlink => 1, :atime => atime, :mtime => mtime, :ctime => ctime })
      elsif @created_files.has_key?(path)
        return @created_files[path]
      elsif @root.file?(path)
        #Set mode from can_write and executable
        mode = 0444
        mode |= 0222 if @root.can_write?(path)
        mode |= 0111 if @root.executable?(path)
        size = size(path)
        atime,mtime,ctime = @root.times(path)
        return RFuse::Stat.file(mode,{ :uid => uid, :gid => gid, :size => size, :atime => atime, :mtime => mtime, :ctime => ctime })
      else
        raise Errno::ENOENT.new(path)
      end
    end
  end
end
# rubocop:enable all

class ZKFS < FuseFS::FuseDir
  DEBUG = false

  def initialize(zk_conn_str)
    @zk_conn_str = zk_conn_str
    @zk = ZK.new(zk_conn_str)
  end

  def directory?(path)
    puts "#directory?(#{path})" if DEBUG
    true unless file?(path) || !(zk.exists?(path))
  end

  def file?(path)
    puts "#file?(#{path})" if DEBUG
    true if file_path?(path) && zk.exists?(zk_path(path))
  end

  def file_path?(path)
    true if scan_path(path)[-1] =~ /.*\.contents$/
  end

  def size(path)
    puts "#size(#{path})" if DEBUG
    zk.stat(zk_path(path)).dataLength
  end

  def executable?(path)
    puts "#executable(#{path})" if DEBUG
    false
  end

  def can_delete?(path)
    puts "#can_delete?(#{path})" if DEBUG
    true
  end

  def can_write?(path)
    puts "#can_write?(#{path})" if DEBUG
    true
  end

  def times(path)
    puts "#times?(#{path})" if DEBUG
    Array.new(3, 0)
    zstats = zk.stat(zk_path(path))
    mtime = zstats.mtime / 1000
    ctime = zstats.ctime / 1000
    # This should be [access time, file modify time, permission change time], but zk
    # does not store the access time so just return mtime.
    [mtime, mtime, ctime]
  end

  # Returns all the non-empty directories of zk as well as a <filename>.contents entry for all non-empty znodes.
  # This maps directly to the info you get when you 'ls' in a directory. As a side effect it will call stat on all of
  # these znodes to determine this information... should be pretty fast.
  def contents(path)
    puts "#contents(#{path})" if DEBUG
    results = []
    root_path = zk_path(path)
    zk.find(root_path) do |z_path|
      if (z_path != root_path)
        z_basename = z_path.split('/').last
        stats = zk.stat(z_path)
        results << "#{z_basename}" if stats.numChildren > 0
        results << "#{z_basename}.contents" if zk.stat(z_path).dataLength > 0
        ZK::Find.prune
      end
    end
    results
  end

  def write_to(path, body)
    puts "#write_to(#{path})" if DEBUG
    znode = zk_path(path)
    create_or_set(znode, body)
  end

  def read_file(path)
    puts "#read_file(#{path})" if DEBUG
    zk.get(zk_path(path))[0].to_s.tap { |i| puts i }
  end

  # This method should not be called until after a file_path? returns true.
  # it is responsible for converting the user-side file system path into the 'true'
  # zookeeper path used to access zookeeper
  def unmangle_leaf_node(path)
    parts = scan_path(path)
    leaf_node_pattern = /(?<basename>.*)\.contents$/
    match = parts[-1].match(leaf_node_pattern)
    return path unless match
    parts[-1] = match['basename']
    result = '/' + parts.join('/')
    # puts "UNMANGLED: #{result}"
    result
  end

  def zk_path(path)
    if file_path?(path)
      unmangle_leaf_node(path)
    else
      path
    end
  end

  # This is the ability to perform a *non-recursive* mkdir.
  def can_mkdir?(path)
    puts "#can_mkdir?(#{path})" if DEBUG
    true unless file_path?(path) || zk.exists?(zk_path(path)) || !zk.exists?(zk_path(split_path(path).first))
  end

  def mkdir(path)
    puts "#mkdir(#{path})" if DEBUG
    zk.mkdir_p(path)
  end

  # Similarly non-recursive
  def can_rmdir?(path)
    puts "#can_rmdir?(#{path})" if DEBUG
    true unless file_path?(path) || !zk.exists?(zk_path(path)) || !zk.children(zk_path(path)).empty?
  end

  def rmdir(path)
    puts "#rmdir(#{path})" if DEBUG
    zk.rm_rf(path)
  end

  def create_or_set(znode, contents)
    zk.mkdir_p(znode) unless zk.exists?(znode)
    zk.set(znode, contents)
  end

  def zk
    if !@zk || !@zk.connected?
      @zk = ZK.new(@zk_conn_str)
    end
    @zk
  end

  def self.mount(zkhost, mountpath)
    root = ZKFS.new(zkhost)
    FuseFS.set_root(root)
    FuseFS.mount_under(mountpath)
    FuseFS.run # This doesn't return until we're unmounted, when it will throw an RFuse::Error.
  rescue RFuse::Error
    $stderr.puts 'Fuse is done. Shutting down.'
  end
end

if $PROGRAM_NAME == __FILE__
  unless ARGV.count == 2
    puts 'USAGE: ruby zkfuse.rb localhost:2181/pillar ./zkmount'
    exit(-1)
  end

  ZKFS.mount(ARGV.shift, ARGV.shift)
end
