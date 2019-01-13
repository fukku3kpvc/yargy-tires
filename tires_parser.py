import json
import xlrd
from yargy import Parser, rule, or_, and_, not_, forward
from yargy.predicates import (
    caseless, type, gram, normalized,
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
                i += 1
                assert match, line
        facts = Tire(vendor=facts_vendor, width=facts_width.width)
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
INT = type('INT').interpretation(
    interp.custom(int)
)
FLOAT = rule(INT.repeatable(), in_({',', '.'}), INT, INT.optional())

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

WIDTH_PREF = {
    'ширина:': 'ширина',
    'Шир:': 'ширина'
}
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
    rule(
        or_(INT, FLOAT).interpretation(Tire.width), SEP,
        or_(INT, FLOAT), SEP, or_(INT, FLOAT)
    )
).interpretation(Tire)

test(
    [VENDOR, WIDTH],
    [data_list[3], Tire(vendor=Vendor(name='HIFLY'), width=185)],
    [data_list[0], Tire(vendor=Vendor(name='Bontyre'), width=33)],
    [data_list[150], Tire(vendor=Vendor(name='Viatti'), width=195)]
)

print()
