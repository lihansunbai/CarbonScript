# -*- coding: utf-8 -*-
__metaclass__ = type


# Class defination
class class_a():
    def __init__(self):
        self.aa = 12345

    @classmethod
    def init_another(self, bb):
        self.bb = bb
        return class_a()
   
    bb = 110


if __name__ == '__main__':

    # First instance
    # Using the default constructor
    cc = class_a()
    print('First instance:')
    print(cc.bb)
    print(cc.aa)


    # Second instance
    # Using 'init_another' constructor
    dd = class_a.init_another(9)
    print('Second instance:')
    print(dd.bb)
    print(dd.aa)
    print('After: \nin first instance')
    print(cc.bb)
    print(cc.aa)
