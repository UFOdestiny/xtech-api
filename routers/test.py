import time
import threading


class Singleton(type):
    def __call__(cls, *args, **kwargs):
        if hasattr(cls, "_instance"):
            return cls._instance
        cls._instance = type.__call__(cls, *args, **kwargs)
        return cls._instance


class A(metaclass=Singleton):
    pass


class IsIntegar(type):
    def __new__(mcs, name, bases, attrs):
        if type(name) == int:
            return super().__new__(mcs, name, bases, attrs)
        else:
            return False


class IsIntegar_metaclass(type):
    def __new__(mcs, name, bases, attrs):
        print(mcs)
        print(name)
        print(bases)
        print(attrs)
        return super().__new__(mcs, name, bases, attrs)

    def __call__(cls, *args, **kwargs):
        if type(args[0]) != int:
            return False
        return type.__call__(cls, *args, **kwargs)


class IsIntegar(metaclass=IsIntegar_metaclass):
    def __init__(self, number):
        self.number = number


num1 = IsIntegar(5)
print(type(num1))
print(num1.number)

num2 = IsIntegar(5.1)
print(type(num2))
print(num2)


class Singleton(type):
    def __call__(cls, *args, **kwargs):
        if hasattr(cls, "_instance"):
            return cls._instance
        cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class A(metaclass=Singleton):
    def __init__(self):
        time.sleep(1)


def task():
    a = A()
    print(a)


for i in range(10):
    t = threading.Thread(target=task)
    t.start()


class Singleton(type):
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if hasattr(cls, "_instance"):
                return cls._instance
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class A(metaclass=Singleton):
    def __init__(self):
        time.sleep(1)


def task():
    a = A()
    print(id(a))


for i in range(10):
    t = threading.Thread(target=task)
    t.start()

if __name__ == "__main__":
    print(id(A()))
    print(id(A()))
