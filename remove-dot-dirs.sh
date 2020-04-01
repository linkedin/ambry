#! /bin/bash

function mvdotdir() {
  OLDDIR=$(dirname $1)
  NEWDIR=${OLDDIR//./\/}
  if [ "$OLDDIR" != "$NEWDIR" ]
  then
    echo moving $1 to $NEWDIR
    mkdir -p $NEWDIR
    git mv $1 $NEWDIR
  fi
}

find -f **/src/*/java -type f | while read file; do mvdotdir "$file"; done
