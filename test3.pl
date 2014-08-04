#!/usr/bin/perl
use strict;
my $pl = 0;
my @array;
my $i;
my $max = 10000000 ;
for($i=0; $i<=$max; $i++){
    $array[$i]=$i;
}

for($i=$max; $i>0; $i--){
    my $rand =int(rand($i+1));
    my $tmp = $array[$i];
    $array[$i] = $array[$rand];
    $array[$rand] = $tmp;
    print "$array[$i],\"test$array[$i]\"\n"
}
print "$array[0],\"test",$array[0],"\""
