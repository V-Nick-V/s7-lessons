import sys
from datetime import datetime, timedelta

def input_paths(date, depth):

# Вычитаем дни из даты
    end = datetime.strptime(date, "%Y-%m-%d")
    start = end - timedelta(days=depth-1)
    
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days+1)]
    
    paths = list()
    
    for date in date_generated:
        tag_date = date.strftime("%Y-%m-%d")
        paths.append(f"/user/nickperegr/data/events/date={tag_date}/event_type=message")
    
    return paths

def main():
    print(input_paths('2022-05-25', 5))
