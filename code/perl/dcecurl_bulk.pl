#!/usr/bin/perl -w
# This script is example code to bulk get data from DCE service on specific date / version. It is tested on perl 5, version 18, subversion 2 (v5.18.2).

# Prerequisite
# 1. You should have perl v5.18.2 or later installed.
# 2. You should have SHA module installed.
# 3. You should have JSON module installed. (For mac os, running "brew install cpanm"  and "sudo cpanm install JSON")


#
# Put x-api-key/shared secret keys to replace 'x-api-key' and 'shared secret' in the code.
#
#   my %envs = (
#    stg  => {xApiKey => 'x-api-key', secret => 'shared secret'},
#    prod  => {xApiKey => 'x-api-key', secret => 'shared secret'}
#   );
#
#

# --env=<env> : stg or prod (must)
# --date=<date> : date like 2016-08-22 (must)
# --version=<version> : version of data, like v1, v2, ... (must)
# --dest=<dest> : target folder to store data. Current folder '.' is used if not specified. (optional)
# --cat=<category> : category data to download. If not specified, all data are downloaded. (optional)
#    
#
# ./dcecurl_bulk.pl --env=<env> --date=<date> --version=<version> --dest=<dest>
# EX. ./dcecurl.pl --env=stg --date=2016-08-22 --version=v1 --dest=.
# This will download all data of v1 on 2016-08-22 . Downloaded files can be found at <dest>/<category>, like ./answers/, ./questions/, ./reviews/

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

print $#ARGV;
if ($#ARGV < 2 || $#ARGV > 4)
{
  print "\nUsage: $0 --env=<env> --date=<date> -version=<version> --dest=<dest> --cat=<category>\n";
  exit;
}

# save arguments following -e or --env in the scalar $host
# the '=s' means that an argument follows the option
# they can follow by a space or '=' ( --env=stg )
GetOptions( 'env=s' => \my $env 
          , 'date=s' => \my $date  
          , 'version=s' => \my $version
          , 'dest=s' => \my $dest
          , 'cat=s' => \my $cat
          );

if(not defined $env)
{
    print "\n--env=<env> must be specified\n";
    exit;
}

if(not defined $date)
{
    print "\n--date=<date> must be specified\n";
    exit;
}

if(not defined $version)
{
    print "\n--version=<version> must be specified\n";
    exit;
}

if (not defined $dest)
{
  $dest='.';
}

chdir($dest);

my %hosts = (
  stg  => 'data-stg.nexus.bazaarvoice.com',
  prod => 'data.nexus.bazaarvoice.com'
);

my %envs = (
    stg  => {xApiKey => 'x-api-key', secret => 'shared secret'},
    prod  => {xApiKey => 'x-api-key', secret => 'shared secret'}
);

my $timestamp = (time . "000");
my $path = "path=/manifests/$date/$version/manifest.json";
my $message = "$path&x-api-key=$envs{$env}{'xApiKey'}&timestamp=$timestamp";
my $args = "-L";
my $url = "$hosts{$env}/v1/dce/data?$path";
utf8::encode($message);
#print "$message\n"; #debug
my $secret  = $envs{$env}{'secret'};
utf8::encode($secret);
my $sign = hmac_sha256_hex($message, $secret);
#print "$sign\n"; #debug

my $headers                 =  HTTP::Headers->new(
  Host                      => $hosts{$env},
  'x-api-key'               => $envs{$env}{'xApiKey'},
  'BV-DCE-ACCESS-SIGN'      => $sign,
  'BV-DCE-ACCESS-TIMESTAMP' => $timestamp
);
my $req                     =  HTTP::Request->new('GET', $hosts{$env}, $headers);

my $ua = LWP::UserAgent->new;


# get manifest.json
my $cmd="curl -s $args '$url' -H 'Host: $hosts{$env}' -H 'x-api-key: $envs{$env}{'xApiKey'}' -H 'BV-DCE-ACCESS-SIGN: $sign' -H 'BV-DCE-ACCESS-TIMESTAMP: $timestamp'";

print $cmd;
print "\n";
print "\n";
my $decoded_json = decode_json( `$cmd` );
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
    
    print "####################### $key ######################\n";
    if (! -d $key) {
      mkdir $key;
    }
    chdir($key);
    foreach my $f( keys %{$decoded_json->{"$key"}}) 
    { 
      print "start to download $f ...\n\n"; 
      $timestamp = (time . "000");
      $path = "path=$f";
      $message = "$path&x-api-key=$envs{$env}{'xApiKey'}&timestamp=$timestamp";
      $args = "-LO";
      $url = "$hosts{$env}/v1/dce/data?$path";
      utf8::encode($message);
      $sign = hmac_sha256_hex($message, $secret);
      $headers                 =  HTTP::Headers->new(
        Host                      => $hosts{$env},
        'x-api-key'               => $envs{$env}{'xApiKey'},
        'BV-DCE-ACCESS-SIGN'      => $sign,
        'BV-DCE-ACCESS-TIMESTAMP' => $timestamp
      );
      $req                     =  HTTP::Request->new('GET', $hosts{$env}, $headers);

      $ua = LWP::UserAgent->new;

      $cmd="curl -s $args '$url' -H 'Host: $hosts{$env}' -H 'x-api-key: $envs{$env}{'xApiKey'}' -H 'BV-DCE-ACCESS-SIGN: $sign' -H 'BV-DCE-ACCESS-TIMESTAMP: $timestamp'";
      print `$cmd`;
      print "Done.\n\n";

    }
    # return to parent directory
    chdir("..");
}