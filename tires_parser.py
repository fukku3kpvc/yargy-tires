import json
import xlrd
from yargy import Parser, rule, or_, and_, not_, forward
from yargy.predicates import (
    caseless, type as type_, gram, normalized,
    in_, in_caseless, dictionary, eq
)
from yargy.pipelines import caseless_pipeline, morph_pipeline
from yargy.interpretation import fact, attribute
from yargy import interpretation as interp
from yargy.tokenizer import MorphTokenizer


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


def test(rule_list, *tests):
    i = 0
    for line, etalon in tests:
        print(line)
        for rule_ in rule_list:
            parser = Parser(rule_)
            matches = list(parser.findall(line))
            print(len(matches))
            if matches:
                match = matches[0]
                if i == 0:  # VENDOR
                    facts_vendor = match.fact
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
                i += 1
                assert match, line
        facts = Tire(vendor=facts_vendor, width=facts_width.width, profile=facts_profile.profile,
                     diameter=facts_diameter.diameter, max_speed_index=facts_speed.max_speed_index,
                     max_load_index=facts_load.max_load_index)
        print(facts)
        assert etalon == facts, facts
        # FACTS_TO_NONE_TYPE
        facts_vendor = Vendor()
        facts, facts_width, facts_profile, facts_diameter, facts_speed, facts_load = \
            Tire(), Tire(), Tire(), Tire(), Tire(), Tire()
        i = 0


tires_vendors_path = 'tires.vendors.json'
xls_path = '06.07.18 ДАКАР Уфа.xls'
# xls_path = 'Прайс_Колобокс_Шины_2018-07-07 (XLS).xls'
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
    interp.custom(float)
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
        or_(INT, FLOAT).interpretation(Tire.width), SEP,
        or_(INT, FLOAT).interpretation(Tire.profile), SEP,
        or_(INT, DIAMETER_WITH_LETTER)
    ),
    rule(
        or_(INT, FLOAT).interpretation(Tire.width), SEP,
        or_(INT, FLOAT).interpretation(Tire.profile), eq('R'),
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
    STRUCTURE
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
    STRUCTURE
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
    INT, rule(INT, eq('/'), INT)
)
MAX_LOAD_INDEX = or_(
    rule(
        STRUCTURE, MAX_LOAD.interpretation(Tire.max_load_index), MAX_SPEED
    ),
    rule(
        STRUCTURE, MAX_SPEED, MAX_LOAD.interpretation(Tire.max_load_index)
    )
).interpretation(Tire)

test(
    [VENDOR, WIDTH, PROFILE, DIAMETER, MAX_SPEED_INDEX, MAX_LOAD_INDEX],
    [data_list[3], Tire(vendor=Vendor(name='HIFLY'), width=185, profile=60, diameter=15,
                        max_speed_index='T', max_load_index=84)],
    [data_list[0], Tire(vendor=Vendor(name='Bontyre'), width=33, profile=12.5, diameter=15,
                        max_speed_index='Q', max_load_index=108)],
    [data_list[150], Tire(vendor=Vendor(name='Viatti'), width=195, profile=80, diameter=14,
                          max_speed_index='C', max_load_index=None)]
)

print()
