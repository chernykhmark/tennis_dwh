
from bs4 import BeautifulSoup
import re


def safe_extract(extract_func, default=None):
    try:
        result = extract_func()
        return result if result is not None else default
    except (AttributeError, IndexError, KeyError, ValueError, TypeError):
        return default


def parse_percentage_value(text: str):
    if not text:
        return None, None, None
    try:

        match = re.search(r'(\d+)%?\s*\(?(\d+)/(\d+)\)?', text)
        if match:
            percentage = int(match.group(1))
            numerator = int(match.group(2))
            denominator = int(match.group(3))
            return percentage, numerator, denominator

        match = re.search(r'(\d+)%', text)
        if match:
            return int(match.group(1)), None, None

        if text.isdigit():
            return None, int(text), None
    except:
        pass
    return None, None, None


def parse_simple_value(text: str):
    try:
        if text and text.isdigit():
            return int(text)
        elif text and text.endswith('%'):
            return int(text.split('%')[0])
    except:
        pass
    return None


def safe_find_all(element, selector, **kwargs):
    try:
        if kwargs:
            return element.find_all(selector, **kwargs)
        else:
            return element.find_all(selector)
    except:
        return []


def ensure_all_fields(data):
    """Гарантирует наличие всех полей в словаре data"""
    statistical_fields = [
        'aces_home', 'aces_away', 'double_faults_home', 'double_faults_away',
        'first_serve_percentage_home', 'first_serve_percentage_away',
        'first_serve_points_won_home', 'first_serve_points_total_home',
        'first_serve_points_won_away', 'first_serve_points_total_away',
        'second_serve_points_won_home', 'second_serve_points_total_home',
        'second_serve_points_won_away', 'second_serve_points_total_away',
        'break_points_saved_home', 'break_points_total_home',
        'break_points_saved_away', 'break_points_total_away',
        'first_return_points_won_home', 'first_return_points_total_home',
        'first_return_points_won_away', 'first_return_points_total_away',
        'second_return_points_won_home', 'second_return_points_total_home',
        'second_return_points_won_away', 'second_return_points_total_away',
        'break_points_converted_home', 'break_points_converted_total_home',
        'break_points_converted_away', 'break_points_converted_total_away',
        'service_points_won_home', 'service_points_total_home',
        'service_points_won_away', 'service_points_total_away',
        'return_points_won_home', 'return_points_total_home',
        'return_points_won_away', 'return_points_total_away',
        'total_points_won_home', 'total_points_total_home',
        'total_points_won_away', 'total_points_total_away',
        'last_10_balls_won_home', 'last_10_balls_won_away',
        'match_points_saved_home', 'match_points_saved_away',
        'service_games_won_home', 'service_games_total_home',
        'service_games_won_away', 'service_games_total_away',
        'return_games_won_home', 'return_games_total_home',
        'return_games_won_away', 'return_games_total_away',
        'total_games_won_home', 'total_games_total_home',
        'total_games_won_away', 'total_games_total_away'
    ]

    for field in statistical_fields:
        if field not in data:
            data[field] = None


def add_data_to_ods(obj):

    try:
        scheduled_str = obj[0]
        finished_str = obj[1]
        result_score_str = obj[2]
        game_id_str = obj[3]
        # Основной код с безопасным извлечением
        sheduled = BeautifulSoup(scheduled_str, "html.parser")
        try:
            resulted = BeautifulSoup(result_score_str, "html.parser")
        except:
            resulted = None
        finished = BeautifulSoup(finished_str, "html.parser")

        data = {}

        data['game_id'] = game_id_str

        # Тур и раунд
        data['tour'] = safe_extract(lambda: [
            li.get_text() for li in sheduled.find('nav', {'data-testid': 'wcl-breadcrumbs'}).find_all('li')
        ][1])
        if data['tour'] in 'TEAMS - MEN' or data['tour'] in 'TEAMS - WOMEN':
            return False

        data['round'] = safe_extract(lambda: [
            li.get_text() for li in sheduled.find('nav', {'data-testid': 'wcl-breadcrumbs'}).find_all('li')
        ][2])

        # Дата и время
        data['datetime'] = safe_extract(
            lambda: sheduled.find('div', class_='duelParticipant__startTime').get_text()
        )

        # Информация о домашнем игроке
        data['player_id_home'] = safe_extract(
            lambda: sheduled.find('div', class_='duelParticipant__home').find('a').get('href').split('/')[-2]
        )

        data['player_link_home'] = safe_extract(
            lambda: 'https://www.flashscore.co.uk' + sheduled.find('div', class_='duelParticipant__home').find('a').get('href')
        )

        data['player_name_home'] = safe_extract(
            lambda: [name.get_text() for name in sheduled.find_all('div', class_='participant__participantName')][0]
        )

        data['player_rank_home'] = safe_extract(
            lambda: int(sheduled.find('div', class_='duelParticipant__home')
                        .find('div', class_='participant__participantRank')
                        .get_text().split(' ')[1][:-1])
        )

        # Информация о гостевом игроке
        data['player_id_away'] = safe_extract(
            lambda: sheduled.find('div', class_='duelParticipant__away').find('a').get('href').split('/')[-2]
        )

        data['player_link_away'] = safe_extract(
            lambda: 'https://www.flashscore.co.uk' + sheduled.find('div', class_='duelParticipant__away').find('a').get('href')
        )

        data['player_name_away'] = safe_extract(
            lambda: [name.get_text() for name in sheduled.find_all('div', class_='participant__participantName')][1]
        )

        data['player_rank_away'] = safe_extract(
            lambda: int(sheduled.find('div', class_='duelParticipant__away')
                        .find('div', class_='participant__participantRank')
                        .get_text().split(' ')[1][:-1])
        )

        # Коэффициенты
        data['player_odd_home'] = safe_extract(
            lambda: [round(float(part.split('/')[0]) / float(part.split('/')[1]), 2)
                     for part in [odd.get_text() for odd in sheduled.find('div', class_='wclOddsContent prematchButtonVisible')
                .find_all('div', {'data-state': "closed"})]][0]
        )

        data['player_odd_away'] = safe_extract(
            lambda: [round(float(part.split('/')[0]) / float(part.split('/')[1]), 2)
                     for part in [odd.get_text() for odd in sheduled.find('div', class_='wclOddsContent prematchButtonVisible')
                .find_all('div', {'data-state': "closed"})]][1]
        )

        # H2H статистика
        h2h = safe_extract(lambda: sheduled.find_all('div', class_='h2h__section section'))

        data['wins_in_last_5_games_home'] = safe_extract(
            lambda: len([w for w in [win.get_text() for win in h2h[0].find_all('span', class_='h2h__icon')] if w in 'W'])
        )

        data['wins_in_last_5_games_away'] = safe_extract(
            lambda: len([w for w in [win.get_text() for win in h2h[1].find_all('span', class_='h2h__icon')] if w in 'W'])
        )

        winners_list = safe_extract(
            lambda: [winner.get_text() for winner in h2h[2].find_all('span', class_='h2h__participantInner winner')]
        )

        data['wins_in_h2h_home'] = safe_extract(
            lambda: len([winner for winner in winners_list if data.get('player_name_home') in winner])
        )

        data['wins_in_h2h_away'] = safe_extract(
            lambda: len([winner for winner in winners_list if data.get('player_name_away') in winner])
        )

        # Победитель
        winner_name = safe_extract(
            lambda: finished.find('div', class_='duelParticipant--winner').find('div',
                                                                                class_='participant__participantName').get_text()
        )

        data['is_winner_home'] = data.get('player_name_home', '') in winner_name
        data['is_winner_away'] = data.get('player_name_away', '') in winner_name

        # Результат матча
        data['result_score_home'] = safe_extract(
            lambda: {
                [span.get_text() for span in resulted.find_all('span', {'data-testid': 'wcl-matchRowScore'})][0]:
                    [span.get_text()[0] for span in resulted.find_all('div', class_='event__part--home')]
            }
        )

        data['result_score_away'] = safe_extract(
            lambda: {
                [span.get_text() for span in resulted.find_all('span', {'data-testid': 'wcl-matchRowScore'})][1]:
                    [span.get_text()[0] for span in resulted.find_all('div', class_='event__part--away')]
            }
        )

        # Статистика матча
        stat_rows = safe_find_all(finished, 'div', class_='wcl-row_2oCpS', attrs={'data-testid': 'wcl-statistics'})

        field_mapping = {
            'Aces': 'aces',
            'Double Faults': 'double_faults',
            '1st Serve Percentage': 'first_serve_percentage',
            '1st Serve Points Won': ('first_serve_points_won', 'first_serve_points_total'),
            '2nd Serve Points Won': ('second_serve_points_won', 'second_serve_points_total'),
            'Break Points Saved': ('break_points_saved', 'break_points_total'),
            '1st Return Points Won': ('first_return_points_won', 'first_return_points_total'),
            '2nd Return Points Won': ('second_return_points_won', 'second_return_points_total'),
            'Break Points Converted': ('break_points_converted', 'break_points_converted_total'),
            'Service Points Won': ('service_points_won', 'service_points_total'),
            'Return Points Won': ('return_points_won', 'return_points_total'),
            'Total Points Won': ('total_points_won', 'total_points_total'),
            'Last 10 Balls': 'last_10_balls_won',
            'Match Points Saved': 'match_points_saved',
            'Service Games Won': ('service_games_won', 'service_games_total'),
            'Return Games Won': ('return_games_won', 'return_games_total'),
            'Total Games Won': ('total_games_won', 'total_games_total')
        }

        for row in stat_rows:
            category_elem = safe_extract(lambda: row.find('div', class_='wcl-category_6sT1J'))
            if not category_elem:
                continue

            category_name = safe_extract(lambda: category_elem.get_text(strip=True))
            if not category_name or category_name not in field_mapping:
                continue

            home_value_elem = safe_extract(lambda: row.find('div', class_='wcl-homeValue_3Q-7P'))
            away_value_elem = safe_extract(lambda: row.find('div', class_='wcl-awayValue_Y-QR1'))

            home_text = safe_extract(lambda: home_value_elem.get_text(strip=True))
            away_text = safe_extract(lambda: away_value_elem.get_text(strip=True))

            field = field_mapping[category_name]

            if isinstance(field, tuple):  # Пара значений
                home_perc, home_won, home_total = parse_percentage_value(home_text)
                data[f'{field[0]}_home'] = home_won
                data[f'{field[1]}_home'] = home_total

                away_perc, away_won, away_total = parse_percentage_value(away_text)
                data[f'{field[0]}_away'] = away_won
                data[f'{field[1]}_away'] = away_total
            else:  # Одиночное значение
                data[f'{field}_home'] = parse_simple_value(home_text)
                data[f'{field}_away'] = parse_simple_value(away_text)

        ensure_all_fields(data)
        return data
    except Exception as e:
        print(f'ошибка {e} в  {game_id_str}')




