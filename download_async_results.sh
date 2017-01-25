#! /bin/bash
#
# USAGE:
#

PATH=:bin:/usr/local/bin:${PATH};

app=$(basename $0);

# Usage message
USAGE="
NAME
    $app - recursively retrieve one or more UFrame asynchronous request result directories

SYNOPSIS
    $app [hupd] file1[ file2 file...]

DESCRIPTION
    Attempts to recursviely retrieve the parent directory and all child files
    located at the url containing '/async_results/', which is parsed from the
    specified JSON response file.

    The default behavior is to download the parent directory and child files
    to the current working directory on the local filesystem under the 
    associated username and timestamped stream.  
    
    For example, assume we want to download the files created by the following
    asynchronous request:
   
    https://opendap-test.oceanobservatories.org/async_results/fujj/20160816T045513-CE05MOAS-GL311-05-CTDGVM000-telemetered-ctdgv_m_glider_instrument

    By default, the files are written to:

    ./fujj/20160816T045513-CE05MOAS-GL311-05-CTDGVM000-telemetered-ctdgv_m_glider_instrument

    where fujj is the user and
    20160816T045513-CE05MOAS-GL311-05-CTDGVM000-telemetered-ctdgv_m_glider_instrument
    is the timestamped stream directory.

    Use the options below to change this behavior.

    -h
        show help message

    -x 
        dry run only. No files downloaded        

    -d
        specify a directory

    -f
        clobber existing directory and contents if it already exists
";

# Default values for options

# Process options
while getopts "hxu:d:p:f" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "x")
            debug=1;
            ;;
#        "u")
#            user=$OPTARG;
#            ;;
        "d")
            root=$OPTARG;
            ;;
#        "p")
#            prefix=$OPTARG;
#            ;;
        "f")
            force=1;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove option from $@
shift $((OPTIND-1));

if [ "$#" -eq 0 ]
then
    echo "No url(s) specified" >&2;
    exit 1;
fi

if [ -z "$root" ]
then
    root=$(pwd);
fi

for f in "$@"
do

    url=$(cat $f | python -m json.tool | grep '/async_results/' | sed 's/[ "]//g');
    if [ -n "$debug" ]
    then
        echo "URL: $url";
        continue;
    fi

    # Get the async results user and timestamped stream directory
    url_user=$(echo $url | awk -F/ '{print $(NF-1)}');
    url_prefix=$(echo $url | awk -F/ '{print $NF}');

    # Override the async results if specified via -u option
    if [ -n "$user" ]
    then
        url_user=$user;
    fi

    # Override the async results if specified via -p option
    if [ -n "$prefix" ]
    then
        url_prefix=$prefix;
    fi

    # Create the destination
    dest="${root}/${url_user}/${url_prefix}";
    if [ -d "$dest" -a -z "$force" ]
    then
        echo "Destination already exists.  Use -f to clobber";
        continue;
    fi

    echo "Fetching: $url";
    echo "Destination: $dest";

    if [ -n "$debug" ]
    then
        wget --spider \
            -r \
            --no-parent \
            -R index.* \
            --no-check-certificate \
            -nH \
            --cut-dirs=3 \
            -P $dest \
            $url/;
    else
        wget -r \
            --no-parent \
            -R index.* \
            --no-check-certificate \
            -nH \
            --cut-dirs=3 \
            -P $dest \
            $url/;
    fi

done

