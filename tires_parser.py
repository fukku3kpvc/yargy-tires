import json
import xlrd
from yargy import Parser, rule, or_
from yargy.predicates import (type as type_, in_, dictionary, eq)
from yargy.pipelines import caseless_pipeline, morph_pipeline
from yargy.interpretation import fact
from yargy import interpretation as interp
from collections import OrderedDict
# from yargy.tokenizer import MorphTokenizer


def get_vendor_dict(vendors_path):
    with open(vendors_path) as file:
        vendors = json.loads(file.read())
    vendors_name = {}
    vendors_id = {}
    for cell in vendors:
        for synonym in cell['Synonyms']:
            vendors_name[synonym] = cell['Name']
            vendors_id[synonym] = cell['Id']
    return vendors_name, vendors_id


def xls_to_list(path):
    rb = xlrd.open_workbook(path, formatting_info=True)
    sheet = rb.sheet_by_index(0)
    list_sheet = []
    current_row = ''
    i = 0
    first_row = sheet.row_values(0)
    for rownum in range(1, sheet.nrows):
        row = sheet.row_values(rownum)
        for cell in row:
            current_row += '{}: {}; '.format(first_row[i], cell)
            i += 1
        list_sheet.append(current_row)
        current_row = ''
        i = 0
    return list_sheet


def show_json(data):
    print(json.dumps(data, indent=2, ensure_ascii=False))


def parse(rule_list, *tests):
    JSON = []
    facts_vendor = Vendor()
    facts_width, facts_profile, facts_diameter, facts_speed, facts_load, facts_season, facts_spikes = \
        Tire(), Tire(), Tire(), Tire(), Tire(), Tire(), Tire()
    i = 0
    for tests_list in tests:
        for line in tests_list:
            print(line)
            for rule_ in rule_list:
                parser = Parser(rule_)
                matches = list(parser.findall(line))
                print(len(matches))
                # При объединении всех правил в одно общее -- метчинг не проходит, если структура строки хоть
                # чуть-чуть изменится. Поэтому, метчинг происходит по каждому правилу отдельно для каждого параметра.
                # Отсюда такая большая и некрасивая структура if-elif
                if matches:
                    match = matches[0]
                    if i == 0:  # VENDOR
                        facts_vendor = match.fact
                        facts_vendor.id = VENDORS_ID[facts_vendor.name]
                        facts_vendor._raw.attributes['id'] = VENDORS_ID[facts_vendor.name]
                    elif i == 1:  # WIDTH
                        facts_width = match.fact
                    elif i == 2:  # PROFILE
                        facts_profile = match.fact
                    elif i == 3:  # DIAMETER
                        facts_diameter = match.fact
                    elif i == 4:  # MAX_SPEED
                        facts_speed = match.fact
                    elif i == 5:  # MAX_LOAD
                        facts_load = match.fact
                    elif i == 6:  # SEASON
                        facts_season = match.fact
                    elif i == 7:  # SPIKES
                        facts_spikes = match.fact
                    i += 1
                    assert match, line
                else:
                    i += 1
                    continue
            if facts_width._raw is None:
                i = 0
                continue
            if facts_vendor._raw is not None:
                facts_width.vendor = facts_vendor
                facts_width._raw.attributes['vendor'] = facts_vendor.as_json
            else:
                facts_width._raw.attributes['vendor'] = OrderedDict([('id', 'None'), ('name', 'None')])
            # Последующие if-else конструкции -- опциональные, добавлены только для валидации None значений
            # при конвертации в JSON формат. Как правило, этого делать не нужно, такие знач. опускаются

            if facts_profile.profile is not None:
                facts_width.profile = facts_profile.profile
                facts_width._raw.attributes['profile'] = facts_profile.profile
            else:
                facts_width._raw.attributes['profile'] = 'None'
            if facts_diameter.diameter is not None:
                facts_width.diameter = facts_diameter.diameter
                facts_width._raw.attributes['diameter'] = facts_diameter.diameter
            else:
                facts_width._raw.attributes['diameter'] = 'None'
            if facts_speed.max_speed_index is not None:
                facts_width.max_speed_index = facts_speed.max_speed_index
                facts_width._raw.attributes['max_speed_index'] = facts_speed.max_speed_index
            else:
                facts_width._raw.attributes['max_speed_index'] = 'None'
            if facts_load.max_load_index is not None:
                facts_width.max_load_index = facts_load.max_load_index
                facts_width._raw.attributes['max_load_index'] = facts_load.max_load_index
            else:
                facts_width._raw.attributes['max_load_index'] = 'None'
            if facts_season.season is not None:
                facts_width.season = facts_season.season
                facts_width._raw.attributes['season'] = facts_season.season
            else:
                facts_width._raw.attributes['season'] = 'None'
            if facts_spikes.spikes is not None:
                facts_width.spikes = facts_spikes.spikes
                facts_width._raw.attributes['spikes'] = facts_spikes.spikes
            else:
                facts_spikes.spikes = 'None'
            show_json(facts_width.as_json)
            JSON.append(facts_width.as_json)
            # assert etalon == facts, facts

            # FACTS_TO_NONE_TYPE
            facts_vendor = Vendor()
            facts_width, facts_profile, facts_diameter, facts_speed, facts_load, facts_season, facts_spikes = \
                Tire(), Tire(), Tire(), Tire(), Tire(), Tire(), Tire()
            i = 0
    return JSON


def to_float(string):
    if string.split(','):
        string = string.replace(',', '.')
        return float(string)
    else:
        return float(string)


def write_output(json_data):
    with open('output_{}.json'.format(xls_path), 'w') as outfile:
        json.dump(json_data, outfile, indent=2, ensure_ascii=False)


# DATA
tires_vendors_path = 'tires.vendors.json'
# xls_path = '06.07.18 ДАКАР Уфа.xls'
xls_path = 'Прайс_Колобокс_Шины_2018-07-07 (XLS).xls'
data_list = xls_to_list(xls_path)

# FACTS
Tire = fact(
    'Tire',
    [
        'vendor', 'width', 'profile',
        'diameter', 'max_speed_index',
        'max_load_index', 'season', 'spikes'
    ]
)
Vendor = fact(
    'Vendor',
    ['id', 'name']
)

# HELPERS
SEP = in_({'-', '/', '|', ':', ';', '.'})
NUM = type_('INT')
INT = NUM.interpretation(interp.custom(int))
FLOAT = rule(NUM.repeatable(), in_({',', '.'}), NUM, NUM.optional()).interpretation(
    interp.custom(to_float)
)

# TIRE_VENDORS
VENDORS_NAME, VENDORS_ID = get_vendor_dict(tires_vendors_path)
VENDOR = rule(
    caseless_pipeline(VENDORS_NAME).interpretation(
        Vendor.name.normalized().custom(VENDORS_NAME.get)
    )
).interpretation(
    Vendor
)

# TIRE_HELPERS
DIAMETER_WITH_LETTER = rule(
    NUM, or_(eq('С'), eq('C')).optional()
)
STRUCTURE = or_(
    rule(
        or_(INT, FLOAT), SEP,
        or_(INT, FLOAT), SEP,
        or_(INT, DIAMETER_WITH_LETTER)
    ),
    rule(
        or_(INT, FLOAT), SEP,
        or_(INT, FLOAT), eq('R'),
        or_(INT, DIAMETER_WITH_LETTER)
    )
)

# TIRE_WIDTH
WIDTH_PIPELINE = morph_pipeline([
    'шир',
    'ширина',
    'width',
    'wid'
])
WIDTH = or_(
    rule(
        WIDTH_PIPELINE, SEP,
        or_(INT, FLOAT).interpretation(Tire.width), SEP
    ),
    or_(
        rule(
            or_(INT, FLOAT).interpretation(Tire.width), SEP,
            or_(INT, FLOAT), SEP,
            or_(INT, DIAMETER_WITH_LETTER)
        ),
        rule(
            or_(INT, FLOAT).interpretation(Tire.width), SEP,
            or_(INT, FLOAT), eq('R'),
            or_(INT, DIAMETER_WITH_LETTER)
        )
    )
).interpretation(Tire)

# TIRE_PROFILE
PROFILE_PIPELINE = morph_pipeline([
    'проф',
    'профиль',
    'prof',
    'profile'
])
PROFILE = or_(
    rule(
        PROFILE_PIPELINE, SEP,
        or_(INT, FLOAT).interpretation(Tire.profile), SEP
    ),
    or_(
        rule(
            or_(INT, FLOAT), SEP,
            or_(INT, FLOAT).interpretation(Tire.profile), SEP,
            or_(INT, DIAMETER_WITH_LETTER)
        ),
        rule(
            or_(INT, FLOAT), SEP,
            or_(INT, FLOAT).interpretation(Tire.profile), eq('R'),
            or_(INT, DIAMETER_WITH_LETTER)
        )
    )
).interpretation(Tire)

# TIRE_DIAMETER
DIAMETER_PIPELINE = morph_pipeline([
    'диаметер',
    'диам',
    'диаметр',
    'diameter',
    'diam',
    'diametr'
])
DIAMETER = rule(
    DIAMETER_PIPELINE, SEP,
    or_(INT, DIAMETER_WITH_LETTER).interpretation(Tire.diameter)
).interpretation(Tire)

# TIRE_MAX_SPEED_INDEX
MAX_SPEED_PIPELINE = morph_pipeline([
    'скор',
    'скорость',
    'макс скорость'
    'макс скор',
    'максимальная скорость',
    'max speed',
    'speed',
    'индекс максимальной скорости',
    'индекс макс скор',
    'max speed index',
    'инд скор',
    'speed index'
])
MAX_SPEED = dictionary({
    'B', 'C', 'D',
    'E', 'F', 'G',
    'J', 'K', 'L',
    'M', 'N', 'P',
    'Q', 'R', 'S',
    'T', 'U', 'H',
    'VR', 'V', 'Z/ZR',
    'Z,ZR', 'W', 'Y'
})
MAX_SPEED_INDEX = or_(
    rule(
        MAX_SPEED_PIPELINE, SEP,
        MAX_SPEED.interpretation(Tire.max_speed_index)
    ),
    rule(
        STRUCTURE, INT.optional(), MAX_SPEED.interpretation(Tire.max_speed_index)
    )
).interpretation(Tire)

# TIRE_MAX_LOAD_INDEX
MAX_LOAD = or_(
    INT, rule(NUM, eq('/'), NUM)
)
MAX_LOAD_INDEX = or_(
    rule(
        STRUCTURE, MAX_LOAD.interpretation(Tire.max_load_index), MAX_SPEED
    ),
    rule(
        STRUCTURE, MAX_SPEED, MAX_LOAD.interpretation(Tire.max_load_index)
    )
).interpretation(Tire)

# TIRE_SEASON
SEASONS_PREFIX = morph_pipeline([
    'сезон',
    'season',
    'сез',
    'сезонность'
])
SEASONS = {
    'лето': 'Summer',
    'зима': 'Winter',
    'летняя': 'Summer',
    'зимняя': 'Winter',
    'л': 'Summer',
    'з': 'Winter',
    'лз': 'Universal',
    'всесезонная': 'Universal',
    'winter': 'Winter',
    'summer': 'Summer',
    'W': 'Winter',
    'S': 'Summer',
    'WS': 'Universal'
}
SEASON = rule(
    SEASONS_PREFIX, SEP, caseless_pipeline(SEASONS).interpretation(
        Tire.season.normalized().custom(SEASONS.get)
    )
).interpretation(Tire)

# TIRE_SPIKES
SPIKES_PREFIX = morph_pipeline([
    'шипы',
    'шип',
    'шипованная'
    'spikes'
])
SPIKES_DICT = {
    'да': 'True',
    'нет': 'False',
    'д': 'True',
    'н': 'False',
    'yes': 'True',
    'no': 'False',
    'y': 'True',
    'n': 'False',
    '+': 'True',
    '-': 'False',
    'true': 'True',
    'false': 'False'
}
SPIKES = rule(
    SPIKES_PREFIX, SEP,
    caseless_pipeline(SPIKES_DICT).interpretation(
        Tire.spikes.normalized().custom(SPIKES_DICT.get)
    )
).interpretation(Tire)

output = parse(
    [VENDOR, WIDTH, PROFILE, DIAMETER, MAX_SPEED_INDEX, MAX_LOAD_INDEX, SEASON, SPIKES],
    data_list
)

write_output(output)
