import os  # To access environment variables
import json  # For parsing JSON data
import urllib.request  # To fetch data from the API
import boto3  # AWS SDK for Python to interact with AWS services
from datetime import datetime, timedelta, timezone  # For date and time handling

# Formats game data into a human-readable string based on game status
def format_game_data(game):
    status = game.get("Status", "Unknown")  # Game status (e.g., Final, InProgress, Scheduled)
    away_team = game.get("AwayTeam", "Unknown")  # Name of the away team
    home_team = game.get("HomeTeam", "Unknown")  # Name of the home team
    final_score = f"{game.get('AwayTeamScore', 'N/A')}-{game.get('HomeTeamScore', 'N/A')}"  # Final score
    start_time = game.get("DateTime", "Unknown")  # Game start time
    channel = game.get("Channel", "Unknown")  # Channel broadcasting the game
    
    # Format quarter scores into a single string
    quarters = game.get("Quarters", [])
    quarter_scores = ', '.join([f"Q{q['Number']}: {q.get('AwayScore', 'N/A')}-{q.get('HomeScore', 'N/A')}" for q in quarters])
    
    # Return different messages based on the game status
    if status == "Final":
        return (
            f"Game Status: {status}\n"
            f"{away_team} vs {home_team}\n"
            f"Final Score: {final_score}\n"
            f"Start Time: {start_time}\n"
            f"Channel: {channel}\n"
            f"Quarter Scores: {quarter_scores}\n"
        )
    elif status == "InProgress":
        last_play = game.get("LastPlay", "N/A")  # Information about the last play
        return (
            f"Game Status: {status}\n"
            f"{away_team} vs {home_team}\n"
            f"Current Score: {final_score}\n"
            f"Last Play: {last_play}\n"
            f"Channel: {channel}\n"
        )
    elif status == "Scheduled":
        return (
            f"Game Status: {status}\n"
            f"{away_team} vs {home_team}\n"
            f"Start Time: {start_time}\n"
            f"Channel: {channel}\n"
        )
    else:
        return (
            f"Game Status: {status}\n"
            f"{away_team} vs {home_team}\n"
            f"Details are unavailable at the moment.\n"
        )

# AWS Lambda function to fetch NBA game data and send updates via SNS
def lambda_handler(event, context):
    # Fetch environment variables
    api_key = os.getenv("NBA_API_KEY")  # API key for SportsData API
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")  # SNS topic ARN for notifications
    sns_client = boto3.client("sns")  # Initialize SNS client
    
    # Get the current time in Central Time (UTC-6)
    utc_now = datetime.now(timezone.utc)
    central_time = utc_now - timedelta(hours=6)  # Adjusting UTC to Central Time
    today_date = central_time.strftime("%Y-%m-%d")  # Format date as YYYY-MM-DD
    
    print(f"Fetching games for date: {today_date}")  # Log the date being queried
    
    # Construct the API URL with the given date and API key
    api_url = f"https://api.sportsdata.io/v3/nba/scores/json/GamesByDate/{today_date}?key={api_key}"
    
    try:
        # Fetch and parse the data from the API
        with urllib.request.urlopen(api_url) as response:
            data = json.loads(response.read().decode())
            print(json.dumps(data, indent=4))  # Log the raw data for debugging
    except Exception as e:
        # Handle errors during API fetch
        print(f"Error fetching data from API: {e}")
        return {"statusCode": 500, "body": "Error fetching data"}
    
    # Format the game data for all games
    messages = [format_game_data(game) for game in data]
    final_message = "\n---\n".join(messages) if messages else "No games available for today."
    
    # Publish the formatted message to SNS
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=final_message,
            Subject="NBA Game Updates"
        )
        print("Message published to SNS successfully.")
    except Exception as e:
        # Handle errors during SNS publishing
        print(f"Error publishing to SNS: {e}")
        return {"statusCode": 500, "body": "Error publishing to SNS"}
    
    # Return success response
    return {"statusCode": 200, "body": "Data processed and sent to SNS"}
