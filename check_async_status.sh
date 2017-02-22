#! /bin/bash --
#
# USAGE:
#

PATH=:bin:/usr/local/bin:${PATH};

app=$(basename $0);

# Usage message
USAGE="
NAME
    $app - check the status of all async requests contained in FILE

SYNOPSIS
    $app [hupd] FILE1[ FILE2 FILE...]

DESCRIPTION
    Attempts to recursviely retrieve the status.txt file located at the url containing 
    /async_results/, which is parsed from the specified JSON response file.

    Options

    -h
        show help message

    -v
        verbose wget output
";

# Default values for options

# Process options
while getopts "hv" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "v")
            verbose=1;
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

for f in "$@"
do

    [ ! -f "$f" ] && continue;

    urls=$(cat $f | python -m json.tool | grep '/async_results/' | sed 's/[ "]//g');
    if [ -z "$urls" ]
    then
        echo "No async download directories found" >&2;
        continue;
    fi

    good_count=0;
    bad_count=0;
    complete_count=0;
    all_status=0;
    for url in $urls
    do

        complete_count=$(( complete_count+1 ));

        # Create link to status.txt file which should exist if the request is
        # complete
        status_url="${url}/status.txt";
   
        # Check to see if the status.txt file exists but do not download it
        if [ -z "$verbose" ]
        then
            wget --spider \
                -r \
                --quiet \
                --no-parent \
                --no-check-certificate \
                -nH \
                --cut-dirs=3 \
                $status_url;
        else
            wget --spider \
                -r \
                --no-parent \
                --no-check-certificate \
                -nH \
                --cut-dirs=3 \
                $status_url;
        fi

        if [ "$?" -eq 0 ]
        then
            good_count=$(( good_count+1 ));
        else
            bad_count=$(( bad_count+1 ));
            all_status=1;
        fi
        
    done

    # Report the results if -v specified
    echo "$good_count/$complete_count requests completed";
    echo "$bad_count/$complete_count requests in process";

done

exit $all_status;
