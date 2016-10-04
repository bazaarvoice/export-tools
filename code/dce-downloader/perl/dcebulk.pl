#!/usr/bin/perl -w
# This script is example code to bulk get data from DCE service on specific date / version. It is tested on perl 5, version 18, subversion 2 (v5.18.2).

use strict;
use utf8;
use Digest::SHA qw(hmac_sha256_hex);
use Data::Dumper;
use JSON qw( decode_json );
use Data::Dumper;  # Perl core module
use Getopt::Long;

require HTTP::Headers;
require HTTP::Request;
require LWP::UserAgent;

if ($#ARGV < 1 || $#ARGV > 4)
{
  print "\nUsage: $0 --key-path=<path to key file> --env=<env> --manifest=<path of manifest.json> --dest=<dest> --type=<category>\n";
  exit;
}

# save arguments following -e or --env in the scalar $host
# the '=s' means that an argument follows the option
# they can follow by a space or '=' ( --env=stg )
GetOptions( 'key-path=s' => \my $keypath
          , 'env=s' => \my $env
          , 'manifest=s' => \my $manifest
          , 'dest=s' => \my $dest
          , 'type=s' => \my $cat
          );

$keypath = '../keys.json' if not defined $keypath;

if(! -f $keypath){
    print "Key file $keypath does not exist!\n" ;
    exit;
}

if(not defined $env)
{
    print "\n--env=<env> must be specified\n";
    exit;
}

my $json_text = do {
   open(my $json_fh, $keypath) or die("Can't open $keypath\n");
   local $/;
   <$json_fh>
};
my $keys = decode_json($json_text);
my $xApiKey = $keys->{"$env"}->{"x-api-key"};
my $sharedKey = $keys->{"$env"}->{"secret"};

if (not defined $dest)
{
  $dest='.';
}
else{
  # Strip last slash if exists
  $dest = $1 if($dest=~/(.*)\/$/);
  if (! -d $dest) {
    print "Destination directory $dest does nto exist";
    exit;
  }
}

chdir($dest);

my %hosts = (
  stg  => 'data-stg.nexus.bazaarvoice.com',
  prod => 'data.nexus.bazaarvoice.com'
);

# get manifest.json
my $cmd=&getCurlCmd($manifest);
my $body = `$cmd`;
if(index($body, "error") != -1 || index($body, "message") != -1 ){
   print "$body\n";
   exit;
}
my $decoded_json = decode_json($body);
print Dumper $decoded_json;

# start to download individual files
foreach my $key( keys %{$decoded_json} ) { 

    if(defined $cat)
    {
      if( $cat ne $key)
      {
          next;
      }
    }
    
    print "####################### Downloading $key ######################\n";
    if (! -d $key) {
      mkdir $key;
    }
    # cd to $dest/$key folder
    chdir($key);
    foreach my $f( keys %{$decoded_json->{"$key"}}) 
    {
      my $fileName = (split '/', $f)[-1];
      print "Download $f to $dest/$key/$fileName\n";

      $cmd=&getCurlCmd($f);
      $body = `$cmd`;
      if(index($body, "error") != -1 || index($body, "message") != -1 ){
         print "$body\n";
      }
      else{
        print "Done.\n";
      }

    }
    # return to parent directory
    chdir("..");
}

sub getCurlCmd{
   # get total number of arguments passed.
  my $path = "path=$_[0]";
  my $args = "-LO";
  if(index($_[0], "manifest.json") != -1 ){
     $args="-L";
  }

  my $timestamp = (time . "000");
  my $message = "$path&x-api-key=$xApiKey&timestamp=$timestamp";
  my $url = "$hosts{$env}/v1/dce/data?$path";
  utf8::encode($message);
  my $sign = hmac_sha256_hex($message, $sharedKey);
  my $headers                 =  HTTP::Headers->new(
    Host                      => $hosts{$env},
    'x-api-key'               => $xApiKey,
    'BV-DCE-ACCESS-SIGN'      => $sign,
    'BV-DCE-ACCESS-TIMESTAMP' => $timestamp
  );
  my $req                     =  HTTP::Request->new('GET', $hosts{$env}, $headers);

  my $ua = LWP::UserAgent->new;
  my $cmd="curl -s $args '$url' -H 'Host: $hosts{$env}' -H 'x-api-key: $xApiKey' -H 'BV-DCE-ACCESS-SIGN: $sign' -H 'BV-DCE-ACCESS-TIMESTAMP: $timestamp'";
  return $cmd;
}