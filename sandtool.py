"""Sand input data processor.

This module retrieves and processes data from the
`clicSAND` interface in order to allow a seemless
integration with `otoole` which is a command-line
interface for advanced users of OSeMOSYS, so that
more efficient open-source solvers can be used to
handle much larger models.

Author: Mario R. Peralta A.
email: Mario.Peralta@ucr.ac.cr

Electric Power & Energy Research Laboratory (EperLab).
"""
import pandas as pd
import yaml
import os


class Sand_Interface():
    """clicSAND excel Interface.

    Attributes
    ----------
    from_year : int
        Initial year.
    to_year : int
        Final year. Inclusive.
    input_sand : dict
        clicSAND data.
    sets_list : list
        Indices (independent variables).
    params_list : list
        Numerical inputs to the model, each parameter
        is a function of the elements in one or more sets.
    config_yaml : dict
        Template with the data structure.
        See method :py:meth:`Sand_Interface.load_config_yaml`.
    sand_yaml : dict
        clicSAND required fields and variables.
        See method :py:meth:`Sand_Interface.write_sand_config_file`.

    Methods
    -------
    read_input_data(path='./InputSand.xlsm')
        Read clicSAND data.
    load_config_yaml(config_file='./config.yaml')
        Template with information of indices and types
        of fields (set, param, result).
    set_input_data()
        Returns suitable data structure.
    write_sand_config_file()
        Generate sand_config.yaml file and sets the attribute
        :py:attr:`Sand_Interface.sand_yaml`.

    Notes
    -----
    It is convention in OSeMOSYS to capitalize set names
    while Pascal case for parameters and results names.
    Furthermore, notice ``from`` and ``to`` years are ``int``,
    however in clicSAND interface year columns are ``str``.
    """

    def __init__(
            self, from_year: int = 2015, to_year: int = 2070
    ):
        """Build interface data object."""
        self.from_year = from_year
        self.to_year = to_year

    def __sets_attr(
            self,
            dict_df: dict[pd.DataFrame]
    ) -> list:
        """Get sets field in clicSAND.

        Columns of the sheet `Parameters` and add
        'YEAR' as another SET.

        Notes
        -----
        The column ``Time indipendent variables``
        applies only for parameters do not depend on YEAR
        like: ``CapacityToActivityUnit``.
        """
        from_year = self.from_year
        cols = dict_df["CapacityToActivityUnit"].columns
        i_year = cols.get_loc(from_year)

        # Skip year cols and use YEAR instead
        sets = list(cols[:i_year]) + ["YEAR"]
        return sets

    def __params_attr(
            self,
            dict_df: dict[pd.DataFrame]
    ) -> list:
        """Get parameters field in clicSAND.

        Unique rows of the clicSAND interface.
        """
        return list(dict_df.keys())

    def __split_emission_region(
        self,
        col: pd.Series
    ) -> tuple[pd.Series]:
        """Break down EMISSION & REGION sets."""
        for n, row in enumerate(col):
            if row == "Region":
                m = n
                i = n + 1
            elif "ResultsPath" in row:
                j = n

        region = col[i:j]
        emission = col[:m]
        return (emission, region)

    def __get_sets(
            self,
            df: pd.DataFrame
    ) -> tuple[pd.Series]:
        """Get codes in sheet ``SETS``.

        Remaining sets (implicit sets) are columns in parameters
        sheet.
        """
        def str_filter(x):
            if x == "Code":
                x = False
            return isinstance(x, str)

        tech = (df["Technologies"][df["Technologies"]
                                   .apply(str_filter)])

        fuel = (df["Commodities"][df["Commodities"]
                                  .apply(str_filter)])

        emission_region = (df["Emissions"][df["Emissions"]
                                           .apply(str_filter)])

        emission, region = self.__split_emission_region(emission_region)

        # Reset indices
        tech = tech.reset_index(drop=True)
        fuel = fuel.reset_index(drop=True)
        emission = emission.reset_index(drop=True)
        region = region.reset_index(drop=True)
        return tech, fuel, emission, region

    def __get_params(
            self,
            df: pd.DataFrame
    ) -> dict:
        """Get data in sheet ``Parameters``.

        Break down DataFrame into a dictionary whose
        keys are the parameters itself. Also define
        :py:attr:`Sand_Interface.sets_list`,
        :py:attr:`Sand_Interface.params_list` and
        :py:attr:`Sand_Interface.config_yaml` attibutes.

        Notes
        -----
        Year fields in clicSAND interface are ``str`` dtype indeed.
        Get rid of **Parameter** column since now
        each parameter is a key.
        Raises
        ------
        FileNotFoundError
            Use ``otoole`` to generate such template file
            in order to continue.
        """
        # Group by "Parameter" column
        grouped = df.groupby("Parameter", as_index=False)

        # Split groups
        dict_df = {param: (group.drop(columns="Parameter")
                           .reset_index(drop=True))
                   for param, group in grouped}
        # Define fields as attributes
        self.sets_list = self.__sets_attr(dict_df)
        self.params_list = self.__params_attr(dict_df)
        config_yaml = self.load_config_yaml()
        if config_yaml:
            self.config_yaml = config_yaml
        else:
            raise FileNotFoundError("Generate config.yaml file.")
        return dict_df

    def __set_sand_data(
            self,
            tech_field: pd.Series,
            fuel_field: pd.Series,
            emission_field: pd.Series,
            region_field: pd.Series,
            param_dict: dict[pd.DataFrame]
    ) -> dict:
        """Set dictionary of clicSAND data.

        This private method defines fields as keys of dictionary.

        Notes
        -----
        Remaining sets are consider :py:obj:`pandas.Series_like`
        (single column) of unique values that are implicit in
        the Parameters sheet.
        See :py:metho:`Sand_Interface.get_implicit_sets`.
        """
        sand_data = {
            "TECHNOLOGY": tech_field,
            "FUEL": fuel_field,
            "EMISSION": emission_field,
            "REGION": region_field,
        }
        # Unpack parameters
        for param, df in param_dict.items():
            sand_data[param] = df

        return sand_data

    def read_input_data(
            self,
            path: str = "./InputSand.xlsm"
    ) -> dict:
        """Read data within clicSAND.

        Return dict with sets and parameters as keys.
        Rename column ``Time indipendent variables``
        to ``VALUE`` and ``REGION2`` to ``REGIONR``.
        """
        dict_df = pd.read_excel(
            io=path,
            sheet_name=["SETS", "Parameters"],
            header=0)
        # Key fist sheet: SETS
        tech, fuel, emission, region = self.__get_sets(df=dict_df["SETS"])
        # Key second sheet: Parameters
        df_params = dict_df["Parameters"]
        # Rename columns
        df_params = df_params.rename(
            columns={"Time indipendent variables": "VALUE"}
        )
        df_params = df_params.rename(
            columns={"REGION2": "REGIONR"}
        )
        # Convert cols representing years to int
        year_cols = [col for col in df_params.columns if col.isdigit()]
        year_mapping = {y_str: int(y_str) for y_str in year_cols}
        df_params = df_params.rename(columns=year_mapping)

        param_dict = self.__get_params(df=df_params)

        self.input_sand = self.__set_sand_data(
            tech_field=tech,
            fuel_field=fuel,
            emission_field=emission,
            region_field=region,
            param_dict=param_dict
        )
        return self.input_sand

    def get_implicit_sets(
            self
    ) -> list:
        """Retrieve implicit sets.

        Return list of those ``set`` types that were not
        explicit declared in "SETS" sheets of clicSAND such as:
        ["MODE_OF_OPERATION", "TIMESLICE", "STORAGE"].

        Raises
        ------
        AttributeError
            Call :py:meth:`Sand_Interface.read_input_data` first.

        Notes
        -----
        "YEAR" is considered an implicit set however is define
        later on :py:meth:`Sand_Interface.set_input_data`
        since is usually known ahead while "VALUE" is a index
        of non-time dependent parameters so there is no need
        the generate a SET sheet in excel template for it
        same as "REGIONR".
        """
        try:
            sets_list = self.sets_list
            input_data = self.input_sand
        except AttributeError as e:
            print(f"AttributeError: {e}")

        implicit_sets = []
        ignore_cols = {"VALUE", "YEAR", "REGIONR"}
        for s in sets_list:
            if s in ignore_cols:
                continue
            elif s not in input_data:
                implicit_sets.append(s)
        return implicit_sets

    def param_sets_dependency(
            self,
            odd_params: list,
            odd_sets: list
    ) -> dict:
        """Establish dependency relationship.

        Useful to get what implicit set the parameter
        depends on.
        """
        config_data = self.config_yaml
        if hasattr(self, 'sand_yaml'):
            config_data = self.sand_yaml
        param_set_dict = {}
        for p in odd_params:
            set_index = config_data[p]["indices"]
            for odd in odd_sets:
                if odd in set_index:
                    if p in param_set_dict:
                        param_set_dict[p].append(odd)
                    else:
                        param_set_dict[p] = [odd]
        return param_set_dict

    def processes_implicit_sets(
            self
    ) -> dict:
        """Give format to implicit sets.

        Filter parameters that depends on such
        indices and takes the column label as a
        :py:obj:`pandas.Series`. Skips ``result`` types since
        input data only considers ``set`` and ``param`` types.
        """
        config_data = self.config_yaml
        implicit_sets = self.get_implicit_sets()
        d_param = []
        for imp in implicit_sets:
            d_param += self.variables_i(
                config_data=config_data,
                set_index=imp
            )
        # Skip result type
        result_types = self.field_type_filter(config_data=config_data,
                                              field="result")
        d_param = [d for d in d_param if d not in result_types]
        # Skip non require parameters
        rm_fields = self.non_required_fields()
        d_param = [d for d in d_param if d not in rm_fields]

        # Dependency relationship
        param_set_dict = self.param_sets_dependency(
            odd_params=d_param,
            odd_sets=implicit_sets
        )

        return param_set_dict

    def set_input_data(
            self
    ) -> dict:
        """Establish implicit sets also as keys."""
        input_data = self.read_input_data()
        param_set_dict = self.processes_implicit_sets()
        imp_sets = self.get_implicit_sets()
        for imp_ind in imp_sets:
            d_params = []
            for param in param_set_dict.keys():
                if imp_ind in param_set_dict[param]:
                    d_params.append(param)
            # Dependent parameters of same implicit SET
            vals = []
            for dp in d_params:
                df = input_data[dp]
                vals += list(df[imp_ind])
            # Unique vals
            vals = list(set(vals))
            vals.sort()
            input_data[imp_ind] = pd.Series(vals)
        # Finally get YEAR
        year_i = self.from_year
        year_j = self.to_year + 1
        years_str = pd.Series(range(year_i, year_j))
        input_data["YEAR"] = years_str
        self.input_sand = input_data
        return input_data

    def load_config_yaml(
            self,
            config_file: str = "./config.yaml"
    ) -> dict:
        """Load ``YAML`` file as a dictionary.

        Template ``*.yaml`` files are generated based
        on a specific version of OSeMOSYS, users
        will need to adapt the template data for
        their own needs.

        Raises
        ------
        FileNotFoundError
            Genegate ``config.yaml`` template file
            and ``data_csv`` directory.
        """
        try:
            with open(config_file, "r") as config_file:
                config_data = yaml.safe_load(config_file)
        except FileNotFoundError as e:
            print(f"FileNotFoundError: {e}")
            return False

        return config_data

    def variables_i(
            self,
            config_data: dict,
            set_index: str = "YEAR",
            result: bool = True
    ) -> list[str]:
        """Filter parameters or results depend on SET."""
        variable_y = []
        if result:
            filter_type = {"param", "result"}
        else:
            filter_type = {"param"}

        for param, feature in config_data.items():
            if feature["type"] in filter_type:
                if set_index in feature["indices"]:
                    variable_y.append(param)
        return variable_y

    def index_independent_variable(
            self,
            set_index: str = "YEAR"
    ) -> list[str]:
        """Filter SET independent variables.

        Retrieve variables (of param and result) do not
        depend on a given SET (index)
        i.e. Filter time independent variables as default
        setting:``set_index="YEAR"``.

        Raises
        ------
        AttributeError
            Call :py:meth:`Sand_Interface.read_input_data` first.
        """
        try:
            sand_params = self.params_list
            config_data = self.config_yaml
        except AttributeError as e:
            print(f"AttributeError: {e}")
            return False

        params_y = self.variables_i(config_data, set_index=set_index)
        non_params_y = [p for p in sand_params if p not in params_y]
        return non_params_y

    def field_type_filter(
            self,
            config_data: dict,
            field: str = "param"
    ) -> dict:
        """Filter set, param or result type."""
        variable_type = []
        for var, fts in config_data.items():
            if fts["type"] == field:
                variable_type.append(var)

        variables = {v: config_data[v] for v in variable_type}

        return variables

    def non_required_fields(
            self,
    ) -> list:
        """Get fields model does not depend on.

        Get those fields (set, param, result) of clicSAND
        input data whose variables are not require to build the model
        hence nither results that depend on such variables
        have to be considered.

        Raises
        ------
        AttributeError
            Call :py:meth:`Sand_Interface.read_input_data` first.
        """
        # Fields in config.yaml
        try:
            config_yaml = self.config_yaml
        except AttributeError as e:
            print(f"AttributeError: {e}")
            return False

        # Fields in clicSAND
        try:
            sand_sets = self.sets_list
            sand_params = self.params_list
        except AttributeError as e:
            print(f"AttributeError: {e}")
            return False

        fields_list = sand_sets + sand_params

        # Sets and Parameters to be removed
        rm_fields = {}
        for ft in config_yaml.keys():
            if config_yaml[ft]["type"] in {"set", "param"}:
                if ft not in fields_list:
                    rm_fields[ft] = config_yaml[ft]

        # Get rid of results depend on removable SET
        rm_vars = []
        rm_sets = {s: fts for s, fts in rm_fields.items()
                   if fts["type"] == "set"}
        for rm_set in rm_sets.keys():
            rm_vars += self.variables_i(
                config_data=config_yaml, set_index=rm_set
            )
        # Concat rm fields
        rm_fields = rm_vars + list(rm_fields.keys())
        return rm_fields

    def __rm_non_fields(self) -> dict:
        """Remove fields the model does not depend on.

        Returns config_dict only with fields the model
        depend on. Remove non required fields in
        directory ``data_csv`` to match this new config_dict.

        Raises
        ------
        AttributeError
            Call :py:meth:`Sand_Interface.read_input_data` first.
        """
        rm_fields = self.non_required_fields()
        try:
            config_yaml = self.config_yaml
        except AttributeError as e:
            print(f"AttributeError: {e}")
            return False

        # Clean up template config file
        sand_yaml = {}
        for var, fts in config_yaml.items():
            if var not in rm_fields:
                sand_yaml[var] = fts

        # Remove *.csv files
        for rm_file in rm_fields:
            rm_path = os.path.join(os.getcwd(),
                                   "data_csv",
                                   f"{rm_file}.csv")
            if os.path.exists(rm_path):
                os.remove(rm_path)

        return sand_yaml

    def write_sand_config_file(
            self
    ) -> None:
        """Generate sand_config.yaml file."""
        sand_yaml = self.__rm_non_fields()
        self.sand_yaml = sand_yaml
        with open("./sand_config.yaml", "w") as yaml_file:
            yaml.dump(
                sand_yaml, yaml_file, default_flow_style=None
            )


class Otoole_Interface():
    """Otoole command-line interface.

    Retrieve data from :py:obj:`Sand_Interface`
    and popuplate excel template generated by `otoole`.

    Attributes
    ----------
    full_names : dict
        Dictionary whose keys are short names of variables
        and its value the full name. Useful to index by key.

    input_otoole : dict
        Migrated data in excel.

    Methods
    -------
    set_full_names()
        Return dict the maps short name to its full name.
        It sets the attribute :py:attr:`full_names`.

    read_excel_data(excel_path='./sandtool.xlsx')
        Read empty template generated by otoole
        following the structure in file ``sand_config.yaml``.

    populate_template()
        Fill empty otoole excel template with
        data in clicSAND interface.

    write_otoole_data(out_path="./sandtool.xlsx")
        Override the template sandtool.xlsx
        with migrated data.
    """

    def __init__(self) -> None:
        """Build otoole object."""
        pass

    def set_full_names(self, sand_data: Sand_Interface) -> dict:
        """Turn short name into full name.

        Excel sheets might truncate a field name using
        the short_name instead, however is demanded to index
        by key using the full label of the field.
        """
        full_names = {}
        for field, fts in sand_data.sand_yaml.items():
            if "short_name" in fts:
                short_n = fts["short_name"]
                full_names[short_n] = field
            else:
                full_names[field] = field
        self.full_names = full_names
        return full_names

    def read_excel_data(
            self,
            excel_path: str = "./sandtool.xlsx"
    ) -> dict[pd.DataFrame]:
        """Read empty otoole excel template."""
        dict_df = pd.read_excel(excel_path, sheet_name=None)
        return dict_df

    def populate_template(
            self,
            sand_data: Sand_Interface
    ) -> dict[pd.DataFrame]:
        """Populate with clicSAND data."""
        dict_df = self.read_excel_data()
        input_data = sand_data.input_sand
        full_names = self.set_full_names(sand_data=sand_data)
        sets = sand_data.sets_list
        params = sand_data.params_list

        for field, df in dict_df.items():
            full_key = full_names[field]
            data_df = input_data[full_key]
            if full_key in sets:
                df["VALUE"] = data_df
            elif full_key in params:
                cols = df.columns
                # Retrieve data
                dict_df[field] = data_df[cols]

        self.input_otoole = dict_df
        return dict_df

    def write_otoole_data(
            self,
            sand_data: Sand_Interface,
            out_path: str = "./sandtool.xlsx",
    ) -> dict[pd.DataFrame]:
        """Override file sandtool.xlsx."""
        input_otoole = self.populate_template(sand_data=sand_data)
        with pd.ExcelWriter(out_path) as writer:
            for sheet, df in input_otoole.items():
                df.to_excel(writer, sheet_name=sheet, index=False)

        return input_otoole


if __name__ == "__main__":
    # Call clicSAND interface
    sand_data = Sand_Interface(2015, 2070)
    input_sand = sand_data.set_input_data()
    sand_data.write_sand_config_file()

    # Call otoole interface
    otoole_data = Otoole_Interface()
    input_otoole = otoole_data.populate_template(sand_data)
