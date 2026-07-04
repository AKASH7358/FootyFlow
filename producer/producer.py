import json
import time
from kafka import KafkaProducer
import os
import requests

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
API_KEY = os.getenv('FOOTBALL_API_KEY')

if not API_KEY:
    raise ValueError("FOOTBALL_API_KEY environment variable is not set")

producer = KafkaProducer(
    bootstrap_servers = KAFKA_BROKER,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')

)

HEADERS = {'X-Auth-Token': API_KEY}
BASE_URL = 'https://api.football-data.org/v4'

def fetch_matches():
    """
    Try to get Live matches first.
    If none are live (most of the time), fall ball to today's FINISHED matches.
    IF none finished today, fall ball to last 7 days of finished matches.
    This ensures the pipeline always has real data to process
    """

    url = f"{BASE_URL}/competitions/WC/matches?status=LIVE"
    response = requests.get(url, headers = HEADERS)
    response.raise_for_status()
    data = response.json()

    if data['matches']:
        print(f"Found {len(data['matches'])} LIVE Matches")
        return data['matches'], 'LIVE'
    
    url = f"{BASE_URL}/competitions/WC/matches?status=FINISHED"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()

    recent = data['matches'][-5:] if data['matches'] else []

    if recent:
        print(f"No live matches. Using {len(recent)} recent FINISHED matches")
        return recent, 'FINISHED'
    
    print("No matches available from API")
    return [], 'NONE'

def format_match(match, status):
    """
    Transform the raw API response into the shape your consumer expects.
    The API returns nested objects — we flatten them into a simple dict.
    """

    home = match['homeTeam']['shortName'] or match['homeTeam']['name']
    away = match['awayTeam']['shortName'] or match['awayTeam']['name']

    home_goals = match['score']['fullTime']['home'] or 0
    away_goals = match['score']['fullTime']['away'] or 0
    score = f"{home_goals}-{away_goals}"

    minute = match.get('minute', 90 if status == 'FINISHED' else 0)

    return {
        'match_id': match['id'],
        'home': home,
        'away': away,
        'score': score,
        'minute': minute,
        'status': status,
        'matchday': match.get('matchday', 0),
        'utc_date': match.get('utcDate', '')
    }

print("Fetching matches from football-data.org...")

matches, status = fetch_matches()

if not matches:
    print("No matches to send. Exiting.")
else:
    print(f"Sending {len(matches)} matches to Kafka topic 'matches'...")

    for match in matches:
        formatted = format_match(match, status)
        producer.send('matches', value=formatted)
        print(f"  Sent: {formatted['home']} vs {formatted['away']} "
              f"({formatted['score']}) — {status}")
        time.sleep(0.5)

    producer.flush()
    print(f"Done. {len(matches)} messages sent to Kafka.")


