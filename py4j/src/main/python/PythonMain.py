class Main(object):
    
    def transform(self, args, emitter):
        json = {'name': 'tyx'}
        emitter.emit(str(json))

    def comput(self, args, emitter):
        names = ""
        for arg in args:
            names += " " + arg['name']
        json = {'names': names}
        emitter.emit(str(json))

    class Java:
        implements = ["py4j.examples.MainInterface"]

from py4j.clientserver import ClientServer, JavaParameters, PythonParameters

main = Main()
gateway = ClientServer(
    java_parameters = JavaParameters(),
    python_parameters = PythonParameters(),
    python_server_entry_point = main
)