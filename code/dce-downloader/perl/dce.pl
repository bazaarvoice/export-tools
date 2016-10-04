#!/usr/bin/perl -w
# This script is example code to query DCE service in BV to get data. It is tested on perl 5, version 18, subversion 2 (v5.18.2).

use strict;
use utf8;
use Digest::SHA qw(hmac_sha256_hex);
use JSON qw( decode_json );
use Data::Dumper;
use Getopt::Long;

require HTTP::Headers;
require HTTP::Request;
require LWP::UserAgent;

if ($#ARGV < 0 || $#ARGV > 3)
{
  print "\nUsage: $0 --env=<env>  --path=<path> --dest=<dest> --key-path=<path to key file>\n";
  exit;
}

# save arguments following -e or --env in the scalar $host
# the '=s' means that an argument follows the option
# they can follow by a space or '=' ( --env=stg )
GetOptions('key-path=s' => \my $keypath
          , 'env=s' => \my $env
          , 'path=s' => \my $path  
          , 'dest=s' => \my $dest
          );

$keypath = '../keys.json' if not defined $keypath;

if(! -f $keypath){
    print "Key file $keypath does not exist!\n" ;
    exit;
}

if (not defined $env)
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

my %hosts = (
  stg  => 'data-stg.nexus.bazaarvoice.com',
  prod => 'data.nexus.bazaarvoice.com'
);

my $timestamp = (time . "000");
my $message = "x-api-key=$xApiKey&timestamp=$timestamp";
my $args = "-L";
my $url = "$hosts{$env}/v1/dce/data";

if (defined $path)           # query param exists
{
  my $path_query = "path=$path";
  $message = "$path_query&$message";
  $url = "$hosts{$env}/v1/dce/data?$path_query";
  chdir($dest);
  $args = "-LO";
  my $fileName = (split '/', $path)[-1];
  print "Download $path to $dest/$fileName\n";
}
else{
  print "Retrieving dates\n";
}

utf8::encode($message);
my $secret  = $sharedKey;
utf8::encode($secret);
my $sign = hmac_sha256_hex($message, $secret);

my $headers                 =  HTTP::Headers->new(
  Host                      => $hosts{$env},
  'x-api-key'               => $xApiKey,
  'BV-DCE-ACCESS-SIGN'      => $sign,
  'BV-DCE-ACCESS-TIMESTAMP' => $timestamp
);
my $req                     =  HTTP::Request->new('GET', $url, $headers);

my $ua = LWP::UserAgent->new;

my $cmd="curl -s $args '$url' -H 'Host: $hosts{$env}' -H 'x-api-key: $xApiKey' -H 'BV-DCE-ACCESS-SIGN: $sign' -H 'BV-DCE-ACCESS-TIMESTAMP: $timestamp'";
my $body = `$cmd`;
print "$body\n";

