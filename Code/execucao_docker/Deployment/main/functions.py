from types import ModuleType
def module_from_file(module_name:str, file_path:str) -> ModuleType:
        """
        Retorna um módulo python a partir de um arquivo, especialmente em ambiente do airflow.

        Parameters
        ----------
            module_name : string
                Nome do módulo a ser carregado.
            file_path : string 
                Caminho do arquivo a ser carregado.

        Returns
        -------
            module : moduleType

        Reference
        -------
            jgoncalves       

        """
        import importlib.util

        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module