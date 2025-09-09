import re
from datetime import date, datetime, timedelta
from functools import partial
from numbers import Number

import pystache
from dateutil.parser import parse
from funcy import distinct

from redash.utils import mustache_render


def _pluck_name_and_value(default_column, row):
    row = {k.lower(): v for k, v in row.items()}
    name_column = "name" if "name" in row.keys() else default_column.lower()
    value_column = "value" if "value" in row.keys() else default_column.lower()

    return {"name": row[name_column], "value": str(row[value_column])}


def _load_result(query_id, org):
    from redash import models

    query = models.Query.get_by_id_and_org(query_id, org)

    if query.data_source:
        query_result = models.QueryResult.get_by_id_and_org(query.latest_query_data_id, org)
        return query_result.data
    else:
        raise QueryDetachedFromDataSourceError(query_id)


def dropdown_values(query_id, org):
    data = _load_result(query_id, org)
    first_column = data["columns"][0]["name"]
    pluck = partial(_pluck_name_and_value, first_column)
    return list(map(pluck, data["rows"]))


def resolve_parameters(parameters, schema):
    updated_parameters = {}
    for key, value in parameters.items():
        definition = next((definition for definition in schema if definition["name"] == key), {})

        if isinstance(value, list):
            updated_parameters[key] = join_parameter_list_values(definition, value)
            continue

        if isinstance(value, str) and value.startswith("d_"):
            param_type = definition.get("type")
            if param_type in ["date", "datetime-local", "datetime-with-seconds"]:
                updated_parameters[key] = _make_dynamic_date(param_type, value)
                continue
            elif param_type in ["date-range", "datetime-range", "datetime-range-with-seconds"]:
                updated_parameters[key] = _make_dynamic_date_range(param_type, value)
                continue

        updated_parameters[key] = value

    return updated_parameters


def join_parameter_list_values(definition, value):
    multi_values_options = definition.get("multiValuesOptions", {})
    separator = str(multi_values_options.get("separator", ","))
    prefix = str(multi_values_options.get("prefix", ""))
    suffix = str(multi_values_options.get("suffix", ""))
    return separator.join([prefix + v + suffix for v in value])


def _collect_key_names(nodes):
    keys = []
    for node in nodes._parse_tree:
        if isinstance(node, pystache.parser._EscapeNode):
            keys.append(node.key)
        elif isinstance(node, pystache.parser._SectionNode):
            keys.append(node.key)
            keys.extend(_collect_key_names(node.parsed))

    return distinct(keys)


def _collect_query_parameters(query):
    nodes = pystache.parse(query)
    keys = _collect_key_names(nodes)
    return keys


def _parameter_names(parameter_values):
    names = []
    for key, value in parameter_values.items():
        if isinstance(value, dict):
            for inner_key in value.keys():
                names.append("{}.{}".format(key, inner_key))
        else:
            names.append(key)

    return names


def _is_number(string):
    if isinstance(string, Number):
        return True
    else:
        float(string)
        return True


def _is_regex_pattern(value, regex):
    try:
        if re.compile(regex).fullmatch(value):
            return True
        else:
            return False
    except re.error:
        return False


def _is_date(string):
    if string.startswith("d_"):
        return True
    parse(string)
    return True


def _is_date_range(obj):
    if isinstance(obj, str) and obj.startswith("d_"):
        return True
    return _is_date(obj["start"]) and _is_date(obj["end"])


def _make_dynamic_date(param_type, value):
    fmt = {
        "date": "%Y-%m-%d",
        "datetime-local": "%Y-%m-%d %H:%M",
        "datetime-with-seconds": "%Y-%m-%d %H:%M:%S",
    }.get(param_type)
    if not fmt:
        raise InvalidParameterError(value)

    now = datetime.now()
    if value == "d_now":
        return now.strftime(fmt)
    if value == "d_yesterday":
        return (now - timedelta(days=1)).strftime(fmt)
    raise InvalidParameterError(value)


def _last_n_days(n_days):
    return lambda today, now: (today - timedelta(days=n_days), now)


def _last_week(today, _):
    last_week_start = today - timedelta(days=today.isoweekday() % 7 + 7)
    return last_week_start, last_week_start + timedelta(days=6)


def _last_month(today, _):
    last_month_end = today - timedelta(days=today.day())
    return last_month_end.replace(day=1), last_month_end


def _last_year(today, _):
    last_year = today.year() - 1
    return date(last_year, 1, 1), date(last_year, 12, 31)


_dynamic_date_range = {
    "d_today": _last_n_days(1),
    "d_yesterday": lambda today, _: (today - timedelta(days=1), today - timedelta(days=1)),
    "d_this_week": lambda today, now: (today - timedelta(days=today.isoweekday() % 7), now),
    "d_this_month": lambda today, now: (today.replace(day=1), now),
    "d_this_year": lambda today, now: (today.replace(month=1, day=1), now),
    "d_last_week": _last_week,
    "d_last_month": _last_month,
    "d_last_year": _last_year,
    "d_last_7_days": _last_n_days(7),
    "d_last_14_days": _last_n_days(14),
    "d_last_30_days": _last_n_days(30),
    "d_last_60_days": _last_n_days(60),
    "d_last_90_days": _last_n_days(90),
    "d_last_12_months": lambda today, now: (today.replace(today.year() - 1) + timedelta(days=1), now),
}


def _make_dynamic_date_range(param_type, value):
    func = _dynamic_date_range.get(value)
    if not func:
        raise InvalidParameterError(value)

    start, end = func(date.today(), datetime.now())

    fmt = {
        "date-range": "%Y-%m-%d",
        "datetime-range": "%Y-%m-%d %H:%M",
        "datetime-range-with-seconds": "%Y-%m-%d %H:%M:%S",
    }.get(param_type)
    if not fmt:
        raise InvalidParameterError(value)

    return {"start": start.strftime(fmt), "end": end.strftime(fmt)}


def _is_value_within_options(value, dropdown_options, allow_list=False):
    if isinstance(value, list):
        return allow_list and set(map(str, value)).issubset(set(dropdown_options))
    return str(value) in dropdown_options


class ParameterizedQuery:
    def __init__(self, template, schema=None, org=None):
        self.schema = schema or []
        self.org = org
        self.template = template
        self.query = template
        self.parameters = {}
        self.resolved_params = {}

    def apply(self, parameters):
        invalid_parameter_names = [key for (key, value) in parameters.items() if not self._valid(key, value)]
        if invalid_parameter_names:
            raise InvalidParameterError(invalid_parameter_names)
        else:
            self.parameters.update(parameters)
            self.resolved_params = resolve_parameters(parameters, self.schema)
            self.query = mustache_render(self.template, self.resolved_params)

        return self

    def _valid(self, name, value):
        if not self.schema:
            return True

        definition = next(
            (definition for definition in self.schema if definition["name"] == name),
            None,
        )

        if not definition:
            return False

        enum_options = definition.get("enumOptions")
        query_id = definition.get("queryId")
        regex = definition.get("regex")
        allow_multiple_values = isinstance(definition.get("multiValuesOptions"), dict)

        if isinstance(enum_options, str):
            enum_options = enum_options.split("\n")

        validators = {
            "text": lambda value: isinstance(value, str),
            "text-pattern": lambda value: _is_regex_pattern(value, regex),
            "number": _is_number,
            "enum": lambda value: _is_value_within_options(value, enum_options, allow_multiple_values),
            "query": lambda value: _is_value_within_options(
                value,
                [v["value"] for v in dropdown_values(query_id, self.org)],
                allow_multiple_values,
            ),
            "date": _is_date,
            "datetime-local": _is_date,
            "datetime-with-seconds": _is_date,
            "date-range": _is_date_range,
            "datetime-range": _is_date_range,
            "datetime-range-with-seconds": _is_date_range,
        }

        validate = validators.get(definition["type"], lambda x: False)

        try:
            # multiple error types can be raised here; but we want to convert
            # all except QueryDetached to InvalidParameterError in `apply`
            return validate(value)
        except QueryDetachedFromDataSourceError:
            raise
        except Exception:
            return False

    @property
    def is_safe(self):
        text_parameters = [param for param in self.schema if param["type"] == "text"]
        return not any(text_parameters)

    @property
    def missing_params(self):
        query_parameters = set(_collect_query_parameters(self.template))
        return (
            set(query_parameters)
            - set(_parameter_names(self.parameters))
            - set(_parameter_names(self.resolved_params))
        )

    @property
    def text(self):
        return self.query


class InvalidParameterError(Exception):
    def __init__(self, parameters):
        parameter_names = ", ".join(parameters)
        message = "The following parameter values are incompatible with their definitions: {}".format(parameter_names)
        super(InvalidParameterError, self).__init__(message)


class QueryDetachedFromDataSourceError(Exception):
    def __init__(self, query_id):
        self.query_id = query_id
        super(QueryDetachedFromDataSourceError, self).__init__(
            "This query is detached from any data source. Please select a different query."
        )
