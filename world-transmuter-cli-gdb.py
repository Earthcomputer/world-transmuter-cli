import gdb

class JavaStringProvider:
    def __init__(self, valobj):
        self.valobj = valobj
        vec = valobj['vec']
        self.length = int(vec['len'])
        data_ptr = vec['buf']['ptr']['pointer']
        self.data_ptr = data_ptr if data_ptr.type.code == gdb.TYPE_CODE_PTR else data_ptr[data_ptr.type.fields()[0]]

    def to_string(self):
        return self.data_ptr.lazy_string(encoding='utf-8', length=self.length)

    @staticmethod
    def display_hint():
        return 'string'

def lookup(valobj):
    if valobj.type.code == gdb.TYPE_CODE_STRUCT and valobj.type.tag == 'java_string::owned::JavaString':
        return JavaStringProvider(valobj)
    else:
        return None

def register_printers(objfile):
    objfile.pretty_printers.append(lookup)

try:
    register_printers(gdb.current_objfile())
except Exception:
    register_printers(gdb.selected_inferior().progspace)
