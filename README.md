BDR 3.0 development tree

BDR 3.0 reworks BDR on top of pglogical and addresses a number of longer term
issues to produce a more comprehensive and maintaintable BDR.

DESIGN
-----

See the document "Building BDR on pglogical"

VPATH BUILDS
-----

vpath builds are supported

     make -f $SRCPATH/Makefile VPATH=$SRCPATH

However, we template `bdr.control` and `bdr_version.h`. PGXS doesn't seem to handle
the control file being on the vpath, so we template them in the source dir for now.
