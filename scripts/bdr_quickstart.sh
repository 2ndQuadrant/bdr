#!/bin/bash
#
# This script is part of the BDR quick-start guide.
#

set -e -u

BDR_PG_REF="${BDR_PG_REF:-bdr-pg/REL9_4_STABLE}"
BDR_PLUGIN_REF="${BDR_PLUGIN_REF:-bdr-plugin/REL1_0_STABLE}"
GIT_URL="${GIT_URL:-https://github.com/2ndQuadrant/bdr.git}"

# If you have an existing local BDR checkout you can put it here to speed the
# clone or set it in the environment:
#
GIT_REFERENCE_REPO="${GIT_REFERENCE_REPO:-}"

BASEDIR="${BASEDIR:-$HOME/2ndquadrant_bdr}"

LOGFILE="$BASEDIR/install.log"

if ! gcc --version  >&/dev/null; then
    echo "You don't seem to have a compiler installed."
    echo
    echo "Fedora/CentOS/RHEL users:"
    echo "    sudo yum groupinstall \"Development Tools\""
    echo "    sudo yum install yum-utils"
    echo "    sudo yum-builddep postgresql"
    echo
    echo "Ubuntu/Debian users:"
    echo "    sudo apt-get install build-essential"
    echo "    sudo apt-get build-dep postgresql"

    exit 1
fi

unexpectedexit() {
    echo "BDR quickstart exited with an unexpected error."
    echo
    echo "Check the log at $LOGFILE for details."
    echo
    echo "Alternately see the manual install instructions in INSTALL.src"
    trap "" EXIT
    exit 1
}

trap unexpectedexit EXIT

clone_or_update() {
    SRCDIR="$1"
    GITREF="$2"

    if test -d $SRCDIR; then
        echo "A directory named $SRCDIR already exists at $BASEDIR/$SRCDIR."
        pushd $SRCDIR

        if ! git remote -v | egrep -q '^origin\s+.*2ndquadrant_bdr\.git'; then
            echo "The data in $BASEDIR/$SRCDIR doesn't seem to be a git clone from 2ndquadrant_bdr.git"
            echo "Move/rename or delete the directory then try again."
            trap "" EXIT
            exit 1
        fi

        if test "" != "$(git status --porcelain)"; then
            echo "The script will now clean and update your git checkout. This will delete"
            echo "the following new or changed files:"
            git status -s
            echo "Is that OK?"

            read -p "Enter to continue, control-C to exit" CONTINUE
            git stash -u
            git clean -fdxq
        fi

        if ! git fetch --tags origin; then
            echo "Failed to update the git repository. Network problems? Look at the git"
            echo "output above for details."
            trap "" EXIT
            exit 1
        fi

    else
        echo "Cloning $GITREF sources from $GIT_URL"
        echo "This may take some time depending on the speed of your connection."

        if ! git clone -b "$GITREF"  "$GIT_URL" $SRCDIR; then
            echo "The git clone of the BDR repository failed. Network issues?"
            echo "Try running the script again. If it fails again, look at the git "
            echo "output to see what went wrong."
            trap "" EXIT
            exit 1
        fi

        pushd $SRCDIR
    fi

    # Ensures we're on the latest rev if $GIT_REF is a branch, without
    # causing issues if we're on a detached head.
    git reset --hard "origin/$GITREF"

    popd
}

mkdir -p $BASEDIR
cd $BASEDIR

clone_or_update bdr-pg-src "$BDR_PG_REF"
pushd bdr-pg-src
./configure --prefix=$BASEDIR/bdr 2>&1 | tee -a "$LOGFILE"
make -s | tee -a "$LOGFILE"
make -s install 2>&1 | tee -a "$LOGFILE"
make -s -C contrib 2>&1 | tee -a "$LOGFILE"
make -s -C contrib install 2>&1 | tee -a "$LOGFILE"
popd

echo "PostgreSQL for BDR compiled and installed."
echo "Preparing BDR extension..."

clone_or_update bdr-plugin-src "$BDR_PLUGIN_REF"
pushd bdr-plugin-src
PATH=$BASEDIR/bdr/bin:$PATH ./configure 2>&1 | tee -a "$LOGFILE"
make -s 2>&1 | tee -a "$LOGFILE"
make -s install 2>&1 | tee -a "$LOGFILE"
popd

echo
echo "---------------------------"
echo "BDR compiled and installed."
echo
echo "Sources at $BASEDIR/bdr-src"
echo "Installed to $BASEDIR/bdr"
echo
echo "Now add it to your PATH:"
echo "    export PATH=$BASEDIR/bdr/bin:\$PATH"
echo "and carry on with the quickstart in the documentation."
echo
echo "WARNING: this is just a toy BDR install for testing and experimentation!"
echo
echo "---------------------------"

trap "" EXIT
