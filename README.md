# network_tracker
keep track of devices on the local network

# Setup

    # update network_tracker_config.py
    cp network_tracker_config.py.example network_tracker_config.py
    # create the schema, function, trigger, tables
    ./commands.py --create-tables
    # update with the current status
    ./commands.py --run-update

# Cron job

This command can be added to a cron job:

    ./commands.py --run-update
