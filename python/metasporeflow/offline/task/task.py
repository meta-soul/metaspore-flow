from abc import ABC, abstractmethod


class Task(ABC):

    def __init__(self,
                 name,
                 type,
                 meta,
                 customer_params,
                 customer_conf_path
                 ):
        self._name = name
        self._type = type
        self._meta = meta
        self._script_path = meta['scriptPath']
        self._customer_params = customer_params
        self._customer_conf_path: str = customer_conf_path

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__,
                               self._name,
                               self._type)

    @abstractmethod
    def _execute(self):
        raise NotImplementedError

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def meta(self):
        return self._meta

    @property
    def customer_params(self):
        return self._customer_params

    @property
    def execute(self):
        return self._execute()

    @property
    def customer_params_yaml_file(self) -> str:
        return self._generate_customer_params_yaml_file_path()

    def _generate_customer_params_yaml_file_path(self) -> str:
        yaml_path = self._customer_conf_path + '/'
        yaml_path += self._name + '.yml'
        return yaml_path
