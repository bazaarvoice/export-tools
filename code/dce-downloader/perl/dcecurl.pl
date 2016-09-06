#!/usr/bin/perl -w
# This script is example code to query DCE service in BV to get data. It is tested on perl 5, version 18, subversion 2 (v5.18.2).

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
# --path=<path> : path to file (optional)
# --dest=<dest> : target folder to store data. Current folder '.' is used if not specified. (optional)
# Usage:
# 1. Get dates available
#    ./dceurl.pl --env=<env>
# EX. ./dcecurl.pl --env=stg
#    
#
# 2. Get data
#    ./dceurl.pl --env=<env> --path=<path> --dest=<dest>
# EX. ./dcecurl.pl --env=stg --path=/manifests/2016-08-22/v1/manifest.json --dest=.


use strict;
use utf8;
use Digest::SHA qw(hmac_sha256_hex);
use Data::Dumper;
use Getopt::Long;

require HTTP::Headers;
require HTTP::Request;
require LWP::UserAgent;

if ($#ARGV < 0 || $#ARGV > 2)
{
  print "\nUsage: $0 --env=<env>  --path=<path> --dest=<dest>\n";
  exit;
}

# save arguments following -e or --env in the scalar $host
# the '=s' means that an argument follows the option
# they can follow by a space or '=' ( --env=stg )
GetOptions( 'env=s' => \my $env 
          , 'path=s' => \my $path  
          , 'dest=s' => \my $dest
          );

if (not defined $env)
{
  print "\n--env=<env> must be specified\n";
  exit;
}

if (not defined $dest)
{
  $dest='.';
}

my %hosts = (
  stg  => 'data-stg.nexus.bazaarvoice.com',
  prod => 'data.nexus.bazaarvoice.com'
);

my %envs = (
    stg  => {xApiKey => 'x-api-key', secret => 'shared secret'},
    prod  => {xApiKey => 'x-api-key', secret => 'shared secret'}
);

my $timestamp = (time . "000");
my $message = "x-api-key=$envs{$env}{'xApiKey'}&timestamp=$timestamp";
my $args = "-L";
my $url = "$hosts{$env}/v1/dce/data";

if (defined $path)           # query param exists
{
  my $path_query = "path=$path";
  $message = "$path_query&$message";
  $url = "$hosts{$env}/v1/dce/data?$path_query";
  chdir($dest);
  $args = "-LO";
}

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
my $req                     =  HTTP::Request->new('GET', $url, $headers);

my $ua = LWP::UserAgent->new;

#my $response = $ua->request($req);
#print Dumper($response);

my $cmd="curl -v $args '$url' -H 'Host: $hosts{$env}' -H 'x-api-key: $envs{$env}{'xApiKey'}' -H 'BV-DCE-ACCESS-SIGN: $sign' -H 'BV-DCE-ACCESS-TIMESTAMP: $timestamp'";

print $cmd;
print "\n";
print "\n";
print `$cmd`;
