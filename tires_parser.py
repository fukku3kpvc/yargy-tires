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


def xls_to_list(xls_path):
    rb = xlrd.open_workbook(xls_path, formatting_info=True)
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
        tokenizer = MorphTokenizer()
        print(list(tokenizer(line)))
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
                i += 1
                assert match, line
        facts = Tire(vendor=facts_vendor, width=facts_width.width, profile=facts_profile.profile)
        print(facts)
        i = 0
        assert etalon == facts, facts


tires_vendors_path = 'tires.vendors.json'
xls_path = '06.07.18 ДАКАР Уфа.xls'
# xls_path = 'Прайс_Колобокс_Шины_2018-07-07 (XLS).xls'
data_list = xls_to_list(xls_path)

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

SEP = in_({'-', '/', '|', ':', ';', '.'})
NUM = type_('INT')
INT = NUM.interpretation(interp.custom(int))
FLOAT = rule(NUM.repeatable(), in_({',', '.'}), NUM, NUM.optional()).interpretation(
    interp.custom(float)
)

VENDORS_NAME, VENDORS_ID = get_vendor_dict(tires_vendors_path)
VENDOR = rule(
    caseless_pipeline(VENDORS_NAME).interpretation(
        Vendor.name.normalized().custom(VENDORS_NAME.get)
    )
).interpretation(
    Vendor
)
# VENDOR = forward().interpretation(
#     Tire
# )
# VENDOR.define(
#     VENDOR_.interpretation(
#         Tire.vendor
#     )
# )

STRUCTURE = rule(
    or_(INT, FLOAT).interpretation(Tire.width), SEP,
    or_(INT, FLOAT).interpretation(Tire.profile), SEP,
    or_(INT, FLOAT)
)

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

test(
    [VENDOR, WIDTH, PROFILE],
    [data_list[3], Tire(vendor=Vendor(name='HIFLY'), width=185, profile=60)],
    [data_list[0], Tire(vendor=Vendor(name='Bontyre'), width=33, profile=12.5)],
    [data_list[150], Tire(vendor=Vendor(name='Viatti'), width=195, profile=80)]
)

print()
