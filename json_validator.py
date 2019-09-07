import json
import copy
from internet_scholar import read_dict_from_url
from datetime import datetime


def merge_dict_keys(dict1, dict2):
    merged = copy.deepcopy(dict1)
    if isinstance(dict2, dict):
        for key, value in dict2.items():
            if dict1.get(key, None) is None:
                if isinstance(value, dict):
                    merged[key] = merge_dict_keys(dict1=dict(), dict2=value)
                elif isinstance(value, list):
                    merged[key] = merge_dict_keys(dict1=list(), dict2=value)
                else:
                    merged[key] = value
            else:
                merged[key] = merge_dict_keys(dict1=dict1[key], dict2=value)
    elif isinstance(dict2, list):
        for element in dict2:
            if len(merged) == 0:
                if isinstance(element, dict):
                    merged.append(merge_dict_keys(dict1=dict(), dict2=element))
                elif isinstance(element, list):
                    merged.append(merge_dict_keys(dict1=list(), dict2=element))
                else:
                    merged.append(element)
            else:
                merged[0] = merge_dict_keys(dict1=merged[0], dict2=element)
    elif dict2 is not None:
        if dict1 is None:
            merged = dict2
        elif dict2 > dict1:
            merged = dict2
    return merged


def missing_dict_keys(dict1, dict2):
    print("Returns a dictionary with the missing keys on the standard or None if None is missing")


def athena_schema(schema, timestamp_format):
    struct = ""
    for key, value in schema.items():
        struct = "{struct},\n`{key}` {value}".format(struct=struct,key=key,value=recursive_schema(value, timestamp_format))
    struct = struct.lstrip(',\n')
    return struct


def orc_schema(schema,timestamp_format):
    new_schema = recursive_schema(schema,timestamp_format).replace('`','')
    new_schema = new_schema.replace('`','').replace(' ','').replace('\n','')
    return new_schema


def recursive_schema(schema, timestamp_format):
    if isinstance(schema, dict):
        if schema == {}:
            return "struct<`dummy_key`: string>"
        else:
            struct = ""
            for key, value in schema.items():
                struct = "{struct},\n`{key}`: {value}".format(struct=struct,key=key,value=recursive_schema(value, timestamp_format))
            struct = struct.lstrip(',\n')
            return "struct<{struct}>".format(struct=struct)
    elif isinstance(schema, list):
        if len(schema) == 0:
            return "array<string>"
        else:
            return "array<{array}>".format(array=recursive_schema(schema[0], timestamp_format))
    elif isinstance(schema, bool):
        return "boolean"
    elif isinstance(schema, float):
        return "float"
    elif isinstance(schema, int):
        if schema > 2147483647:
            return "bigint"
        else:
            return "int"
    elif isinstance(schema, str):
        try:
            datetime.strptime(schema, timestamp_format)
        except ValueError as e:
            return "string"
        else:
            return "timestamp"
    elif schema is None:
        return "string"
    else:
        raise TypeError("Unknown type. It is impossible to determine the schema.")


class JSONValidator:
    def __init__(self, url=None):
        if url is None:
            self.standard = dict()
        else:
            self.standard = read_dict_from_url(url)

    def find_standard_in_file(self, filename):
        standard_file = dict()
        with open(filename, "r") as input_file:
            for line in input_file:
                record_aux = json.loads(line.strip())
                standard_file = merge_dict_keys(standard_file, record_aux)
        self.standard = standard_file

    def missing_keys(self, filename):
        standard_file = dict()
        with open(filename, "r") as input_file:
            for line in input_file:
                record_aux = json.loads(line.strip())
                standard_file = merge_dict_keys(standard_file, record_aux)
        return missing_dict_keys(self.standard, standard_file)

    def add_keys(self, new_keys):
        self.standard = merge_dict_keys(self.standard, new_keys)

    def orc_schema(self):
        return orc_schema(self.standard, timestamp_format='%Y-%m-%d %H:%M:%S')

    def athena_schema(self):
        return athena_schema(self.standard, timestamp_format='%Y-%m-%d %H:%M:%S')

    def save_standard(self, filename):
        with open(filename, "w") as output:
            output.write("%s" % json.dumps(self.standard))


def main():
    json_validator = JSONValidator(url="https://raw.githubusercontent.com/internet-scholar/twitter_stream/master/sample/tweet.json")
    print(json_validator.athena_schema())
    #json_validator = JSONValidator()
    #json_validator.find_standard_in_file('twitter_stream.json')
    #json_validator.save_standard('new_tweet.json')


if __name__ == '__main__':
    main()