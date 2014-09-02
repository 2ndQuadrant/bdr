#!/bin/bash
#
# This script is part of the BDR quick-start guide.
#

set -e -u

GIT_REF="bdr-next"

# If you have an existing local PostgreSQL checkout you can put it here
# to speed the clone or set it in the environment:
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
    trap "" EXIT
    exit 1
}

trap unexpectedexit EXIT

echo "Creating $BASEDIR"
mkdir -p $BASEDIR
cd $BASEDIR

if test -d bdr-src; then
    echo "A directory named bdr-src already exists at $BASEDIR/bdr-src."
    cd bdr-src

    if ! git remote -v | egrep -q '^origin\s+.*2ndquadrant_bdr\.git'; then
        echo "The data in bdr-src doesn't seem to be a git clone from 2ndquadrant_bdr.git"
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
    echo "Cloning the BDR sources from git://git.postgresql.org/git/2ndquadrant_bdr.git"
    echo "This may take some time depending on the speed of your connection."

    if ! git clone git://git.postgresql.org/git/2ndquadrant_bdr.git bdr-src; then
        echo "The git clone of the BDR repository failed. Network issues?"
        echo "Try running the script again. If it fails again, look at the git "
        echo "output to see what went wrong."
        trap "" EXIT
        exit 1
    fi

    cd bdr-src
fi

if ! git checkout "$GIT_REF"; then
    echo "Failed to check out $GIT_REF from git. See the output above for details."
    trap "" EXIT
    exit 1
fi

# Ensures we're on the latest rev if $GIT_REF is a branch, without
# causing issues if we're on a detached head.
git reset --hard "origin/$GIT_REF"

./configure --prefix=$BASEDIR/bdr 2>&1 | tee -a "$LOGFILE"
make install 2>&1 | tee -a "$LOGFILE"
make -C contrib install 2>&1 | tee -a "$LOGFILE"

echo
echo "---------------------------"
echo "BDR compiled and installed."
echo
echo "Sources at $BASEDIR/bdr-src"
echo "Installed to $BASEDIR/bdr"
echo
echo "Now add it to your PATH:"
echo "    export PATH=$BASEDIR/bdr/bin:\$PATH"
echo "and carry on with the quickstart at https://wiki.postgresql.org/wiki/BDR_User_Guide"
echo "---------------------------"

trap "" EXIT
