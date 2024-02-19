# mobsos-event-log-generator

A python server that can create an XES event log from the mobsos database. Also includes the event_reader library that can be used to read the event log and create a pandas dataframe from it.

## Usage

Check the `example.ipynb` file for an example of how to use the event_reader library to read the event log and create a pandas dataframe from it.

## Server

The server is a flask server that can be used to generate event logs for resources and bots.

The server can be started by running the `app.py` file. The server will start on port 8087 by default. The server has the following endpoints:

- `/resource/<resource_id>`: This endpoint can be used to generate an event log for a specific resource. The resource id should be passed as a parameter in the URL. The start and end date can also be passed as parameters. The event log will be generated for the specified resource and time period and will be returned as a file.

- `/resources`: This endpoint can be used to get the event log for a list of resources. The resource ids should be passed as a `resource_ids` in json body of the POST request. The start and end date can be passed as query parameters. The event log will be generated for the specified resources and time period and will be returned as a file.

- `/bot/<botName>`: This endpoint can be used to generate an event log for a bot. The bot name should be passed as a parameter in the URL. The start and end date can also be passed as parameters. The event log will be generated for the specified bot and time period and will be returned as a file.
Note the bot-manager should be passed as a query parameter as the application will fetch the resource ids associated with the bot from the bot-manager.