# -*- coding: utf-8 -*-
__metaclass__ = type


# Class defination
class class_a():
    def __init__(self):
        self.aa = 12345

    @classmethod
    def init_another(cls, bb):
        root_init = cls()
        cls.sbb = bb
        root_init.ssbb = bb
        return root_init

    def set_sbb(self, bb):
        self.bb = bb

    def get_sbb(self):
        return self.bb
    
    sbb = property(get_sbb, set_sbb)
   
    bb = 110


if __name__ == '__main__':

    # # First instance
    # # Using the default constructor
    # cc = class_a()
    # print('First instance:')
    # print(cc.bb)
    # print(cc.aa)


    # # Second instance
    # # Using 'init_another' constructor
    # dd = class_a.init_another(9)
    # print('Second instance:')
    # print(dd.bb)
    # print(dd.aa)
    # print('After: \nin first instance')
    # print(cc.bb)
    # print(cc.aa)

    aaa = class_a.init_another(6)
    print(aaa.sbb)
    print(aaa.ssbb)