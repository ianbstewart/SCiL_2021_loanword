# kill process by name
#PROCESS="tag_mined_tweets.py"
#PROCESS="ipykernel_launcher"
#PROCESS="mine_twitter_archive.py"
#PROCESS="generate_word_counts_from_data.py"
PROCESS="elasticsearch"
PROCESSES=$(ps aux | grep "$PROCESS" | awk '{print $2}')
echo $PROCESSES
kill $PROCESSES
