import logging
from functools import partial

from mongoengine.base import BaseDocument


logger = logging.getLogger(__name__)


class MinimizingException(Exception):
    pass


def minimize(model, *args):
    """
    The aim of this function is to deconstruct a mongoengine document and to keep only the fields and function calls
    stored in the args.

    > If you want to keep a field, write its name in the attributes list like so :
    minimize(my_document, 'one_field')

    > If you want to emit a nested argument you can write a . between the attribute and the nested attribute
    minimize(my_document, 'one_field.sub_field')

    > If you want to emit a function, you can add in the args a list described by the following:
    minimize(my_document, ['function name', list_of_args_in_the_right_order] or ['function name', dict_of_kwargs])

    :param model: the object you want to minimize
    :param args: the elements you want to keep inside the diminished version of your object
    :return: the diminished version of your object
    """
    def change_id_attribute(elem):
        if isinstance(elem, BaseDocument):
            elem = elem.to_mongo()
            elem['id'] = elem.pop('_id', None)

        return elem

    res = {}
    for attribute in args:
        if isinstance(attribute, list):
            val = getattr(model, attribute[0])
            if val is None:
                raise MinimizingException('This method does not exist')
            if attribute[0] not in res:
                res[attribute[0]] = {}

            if isinstance(attribute[1], list):
                res[attribute[0]][','.join(attribute[1])] = val(*attribute[1])
            elif isinstance(attribute[1], dict):
                res[attribute[0]][','.join(attribute[1])] = val(**attribute[1])

        else:
            nested_attributes = attribute.split('.')
            if len(nested_attributes) < 2:
                val = getattr(model, attribute)
                if val is None:
                    raise MinimizingException('This field does not exist')
                if isinstance(val, list):
                    for i in range(len(val)):
                        val[i] = change_id_attribute(val[i])
                else:
                    val = change_id_attribute(val)

                res[attribute] = val

            else:
                val = getattr(model, nested_attributes[0])
                if isinstance(val, BaseDocument):
                    sub_attributes = ['.'.join(nested_attributes[1:])]
                    if nested_attributes[0] not in res:
                        res[nested_attributes[0]] = {}

                    res[nested_attributes[0]].update(minimize(val, sub_attributes))

    if not res:
        res = model

    return res


def reassemble(obj, model_class):
    def return_correct_value(*args, **kwargs):
        return kwargs.get('exe').get(','.join(args))

    method_calls = []
    for attr, val in obj.items():
        if attr not in dir(model_class):
            method_calls.append({attr: val})
            obj.pop(attr)

    res = model_class(**obj)
    for method_call in method_calls:
        for method_name, executions in method_call.items():
            setattr(res, method_name, partial(return_correct_value, exe=executions))

    return res
