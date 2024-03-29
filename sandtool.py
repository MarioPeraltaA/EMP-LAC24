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
    config_path : str
        Path to the config.yaml file generated by otoole.
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
    load_config_yaml()
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
            self,
            from_year: int = 2015,
            to_year: int = 2070,
            config_path : str = "./config.yaml"
    ):
        """Build interface data object."""
        self.from_year = from_year
        self.to_year = to_year
        self.config_path = config_path

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
            input_sand_path, 
    ) -> dict:
        """Read data within clicSAND.

        Return dict with sets and parameters as keys.
        Rename column ``Time indipendent variables``
        to ``VALUE`` and ``REGION2`` to ``REGIONR``.
        """
        dict_df = pd.read_excel(
            io=input_sand_path,
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
            set_label = config_data[p]["indices"]
            for odd in odd_sets:
                if odd in set_label:
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
                set_label=imp
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
            self,
            input_sand_path: str = "./InputSand.xlsm"
    ) -> dict:
        """Establish implicit sets also as keys."""
        input_data = self.read_input_data(input_sand_path)
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
            dtype_set =  self.config_yaml[imp_ind]["dtype"]
            input_data[imp_ind] = pd.Series(vals, dtype=dtype_set)
        # Finally get YEAR
        year_i = self.from_year
        year_j = self.to_year + 1
        years_str = pd.Series(range(year_i, year_j), dtype=int)
        input_data["YEAR"] = years_str
        self.input_sand = input_data
        return input_data

    def load_config_yaml(
            self,
    ) -> dict:
        """Load ``YAML`` file as a dictionary.

        Template ``*.yaml`` files are generated based
        on a specific version of OSeMOSYS, users
        will need to adapt the template data for
        their own needs.

        Raises
        ------
        FileNotFoundError
            Usue otoole to genegate ``config.yaml`` template file
            and ``data_csv`` directory.
        """
        config_path = self.config_path
        try:
            with open(config_path, "r") as config_file:
                config_data = yaml.safe_load(config_file)
        except FileNotFoundError as e:
            print(f"FileNotFoundError: {e}")
            return False

        return config_data

    def variables_i(
            self,
            config_data: dict,
            set_label: str = "YEAR",
            result: bool = True
    ) -> list[str]:
        """Filter parameters or results depend on SET.

        If ``result`` is False, it would filter parameters
        type only.
        """
        variable_y = []
        if result:
            filter_type = {"param", "result"}
        else:
            filter_type = {"param"}

        for param, feature in config_data.items():
            if feature["type"] in filter_type:
                if set_label in feature["indices"]:
                    variable_y.append(param)
        return variable_y

    def index_independent_variable(
            self,
            params_list: list,
            config_data: dict,
            set_label: str = "YEAR"
    ) -> list[str]:
        """Filter SET independent variables.

        Retrieve variables (of param and result) do not
        depend on a given SET (index)
        i.e. Filter time independent variables as default
        setting:``set_label="YEAR"``.
        """

        params_y = self.variables_i(config_data, set_label=set_label)
        non_params_y = [p for p in params_list if p not in params_y]
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
                config_data=config_yaml, set_label=rm_set
            )
        # Concat rm fields
        rm_fields = rm_vars + list(rm_fields.keys())
        return rm_fields

    def __rm_non_fields(self,
                        csv_dir_name: str = "data_csv") -> dict:
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
        config_dir, _ = os.path.split(self.config_path)
        data_csv_root = os.path.join(config_dir, csv_dir_name)
        for rm_file in rm_fields:
            rm_path = os.path.join(data_csv_root, f"{rm_file}.csv")
            if os.path.exists(rm_path):
                os.remove(rm_path)

        return sand_yaml

    def write_sand_config_file(
            self,
            name_file: str = "sand_config.yaml"
    ) -> None:
        """Generate sand_config.yaml file."""
        sand_yaml = self.__rm_non_fields()
        self.sand_yaml = sand_yaml
        sand_config_dir, _ = os.path.split(self.config_path)
        sand_config_path = os.path.join(sand_config_dir, name_file)
        with open(sand_config_path, "w") as yaml_file:
            yaml.dump(
                sand_yaml, yaml_file, default_flow_style=None
            )


class Otoole_Interface():
    """Otoole command-line interface.

    Retrieve data from :py:obj:`Sand_Interface`
    and popuplate excel template generated by `otoole`.

    Attributes
    ----------
    input_otoole_path : str
        Path of the ``sandtool.xlsx`` file.
    config_sand_path : str
        Path of ``sand_config.yaml``  file.
    sand_yaml : dict
        clicSAND required variables (parameters) and
        fields (sets, technologies, regions, emissions).
    full_names : dict
        Dictionary whose keys are short names of variables
        and its value the full name. Useful to index by key.
    input_otoole : dict
        Migrated data in excel.
        See :py:meth:`Otoole_Interface.populate_template`.
    data_prep : dict
        Raw data preparation by
        :py:meth:`Otoole_Interface.read_excel_data_prep`.
        Skip headers may be required for that
        see :py:meth:`Otoole_Interface.skip_rows_df`.
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

    def __init__(
            self,
            input_otoole_path: str = "./sandtool.xlsx",
            config_sand_path: str = "./sand_config.yaml"
    ) -> None:
        """Build otoole object."""
        self.input_otoole_path = input_otoole_path
        self.config_sand_path = config_sand_path
        self.sand_yaml = self.sand_config

    @property
    def sand_config(self):
        """Read ``sand_config.yaml`` file."""
        config_path = self.config_sand_path
        try:
            with open(config_path, "r") as config_file:
                config_data = yaml.safe_load(config_file)
        except FileNotFoundError as e:
            print(f"FileNotFoundError: {e}")
            raise FileNotFoundError("Missing config file.")

        return config_data

    def set_full_names(
            self
    ) -> dict:
        """Turn short name into full name.

        Excel sheets might truncate a field name using
        the short_name instead, however is demanded to index
        by key using the full label of the field.
        """
        full_names = {}
        for field, fts in self.sand_yaml.items():
            if "short_name" in fts:
                short_n = fts["short_name"]
                full_names[short_n] = field
            else:
                full_names[field] = field
        self.full_names = full_names
        return full_names

    def set_short_names(
            self
    ) -> dict:
        """Turn full name into short name.

        Before writing in order to generate .xlsx file
        with original sheet names.
        """
        if hasattr(self, "full_names"):
            full_names = self.full_names
        else:
            full_names = self.set_full_names()

        short_names = {f: s for s, f in full_names.items()}
        return short_names

    def read_excel_data(
            self,
    ) -> dict[pd.DataFrame]:
        """Read empty otoole excel template."""
        excel_path = self.input_otoole_path
        dict_df = pd.read_excel(excel_path, sheet_name=None)
        full_key_mapping = self.set_full_names()
        # Rename keys
        dict_df = {full_key_mapping[var]: df for var, df in dict_df.items()}
        return dict_df

    def populate_template(
            self,
            sand_data: Sand_Interface
    ) -> dict[pd.DataFrame]:
        """Populate with clicSAND data."""
        dict_df = self.read_excel_data()
        input_data = sand_data.input_sand
        sets = sand_data.sets_list
        params = sand_data.params_list

        for field, df in dict_df.items():
            data_df = input_data[field]
            if field in sets:
                df["VALUE"] = data_df
            elif field in params:
                cols = df.columns
                # Retrieve data
                dict_df[field] = data_df[cols]

        self.input_otoole = dict_df
        return dict_df

    def read_excel_data_prep(
        self,
        file_path: str = "./data_prep/Data_prep_HO3.xlsx"
    ) -> dict:
        """Read preparation file."""
        dict_df = pd.read_excel(file_path, sheet_name=None)
        self.data_prep = dict_df
        return dict_df

    def skip_rows_df(
        self,
        df: pd.DataFrame,
        col: str = 'Specified Demand Profile'
    ) -> pd.DataFrame:
        """Reset header and indices (columns)."""
        # Verify if new header is required
        cols = df.columns
        if col in cols:
            return df
        else:
            for i in df.index:
                row = df.iloc[i, :].values
                if col in row:
                    head_i = i
        # New header
        df.columns = df.iloc[head_i]
        # Remove all rows before (inclusive)
        rm_indices = df.index[0:head_i+1]
        df = df.drop(rm_indices)
        # Reset index inplace
        df = df.reset_index(drop=True)
        df = df.rename(
            columns={"Time Independent Parameters": "VALUE"}
        )
        return df

    def variables_i(
            self,
            config_data: dict,
            set_label: str = "YEAR",
            result: bool = True
    ) -> list[str]:
        """Filter parameters or results depend on SET.

        If ``result`` is False, it would filter parameters
        type only.
        """
        variable_y = []
        if result:
            filter_type = {"param", "result"}
        else:
            filter_type = {"param"}

        for param, feature in config_data.items():
            if feature["type"] in filter_type:
                if set_label in feature["indices"]:
                    variable_y.append(param)
        return variable_y

    def replace_set_code(
            self,
            set_label: str = "TECHNOLOGY",
            **kwcodes: dict[str, str]
    ) -> dict[pd.DataFrame]:
        """Update set fields.

        Rename code throughout the data of
        a given set type (label). Arbitrary length parameter ``kwcodes``
        whose key are old code names and values new code names.

        Notes:
        It is require to populate data first.
        """
        sand_config = self.sand_yaml
        input_otoole = self.input_otoole
        # Rename in sets field
        for old_code, new_code in kwcodes.items():
            set_df = input_otoole[set_label]
            set_df.loc[set_df["VALUE"] == old_code] = new_code
            # Update
            input_otoole[set_label] = set_df

            # Rename all parameters
            params_i = self.variables_i(sand_config,
                                        set_label=set_label,
                                        result=False)
            for p in params_i:
                df = input_otoole[p]
                df.loc[df[set_label] == old_code, set_label] = new_code
                # Update
                input_otoole[p] = df

        self.input_otoole = input_otoole
        return input_otoole

    def break_down_df(
            self,
            df: pd.DataFrame,
            category: str = "ParameterID",
            set_type: bool = False
    ) -> dict[pd.DataFrame]:
        """Split data into groups per parameter or set.

        In case if data of a set type it will not drop the column.
        """
        if set_type:
            grouped = df.groupby(category, as_index=False)
            dict_df = {setlabel: (sub_df
                                  .reset_index(drop=True))
                       for setlabel, sub_df in grouped}
        else:
            grouped = df.groupby(category, as_index=False)
            dict_df = {param: (sub_df
                               .drop(columns=category)
                               .reset_index(drop=True))
                       for param, sub_df in grouped}

        return dict_df

    def add_tech(
            self,
            sheet: str,
            new_tech_code: str,
            region: str = "RE1",
            header: str = "ParameterID",
    ) -> dict[pd.DataFrame]:
        """Add and parameterize technology."""
        if hasattr(self, "data_prep"):
            df = self.data_prep[sheet]
        else:
            raise AttributeError("Read data preparation first.")

        df = self.skip_rows_df(df, col=header)
        dict_df = self.break_down_df(df=df, category=header)
        input_otoole = self.input_otoole
        for param, df in dict_df.items():
            df = df[(df['REGION'] == region) & (df['TECHNOLOGY'] == new_tech_code)]
            otoole_df = input_otoole[param]
            cols = otoole_df.columns
            rows = otoole_df['TECHNOLOGY'] == new_tech_code
            rows = (rows) & (otoole_df['REGION'] == region)
            otoole_df_copy = otoole_df.copy()
            otoole_df_copy.loc[rows, :] = df[cols].values
            input_otoole[param] = otoole_df_copy

        self.input_otoole = input_otoole
        return input_otoole

    def add_fuel_param(
            self,
            param: str,
            fuel_code: str,
            region: str,
            sheet: str,
            header: str
    ) -> dict[pd.DataFrame]:
        """Update parameter with fuel dependency.

        Update data of parameters that depend on
        fuel but not on technolgy such as:

            - AccumulatedAnnualDemand
            - RETagFuel
            - ReserveMarginTagFuel
            - SpecifiedAnnualDemand
            - SpecifiedDemandProfile
            - TradeRoute

        Adaptation of cells in the data preparation file
        may be required to ensure seemless integration.
        """
        if hasattr(self, "data_prep"):
            df = self.data_prep[sheet]
        else:
            raise AttributeError("Read data preparation first.")

        sand_config = self.sand_yaml
        if param in self.variables_i(
            sand_config, "TECHNOLOGY", False
        ):
            raise TypeError("Parameter depends on tecnology.")

        input_otoole = self.input_otoole
        df = self.skip_rows_df(df, col=header)
        indices = (df['FUEL'] == fuel_code)
        otoole_df = input_otoole[param]

        cols = otoole_df.columns
        rows = (otoole_df['FUEL'] == fuel_code)
        rows = (rows) & (otoole_df['REGION'] == region)
        otoole_df = otoole_df.copy()
        otoole_df.loc[rows, :] = df.loc[indices, cols]
        input_otoole[param] = otoole_df

        self.input_otoole = input_otoole
        return input_otoole

    def add_single_param(
            self,
            param: str,
            set_label: str,
            region: str,
            sheet: str,
            header: str
    ) -> dict[pd.DataFrame]:
        """Update parameter with unique values per row."""
        if hasattr(self, "data_prep"):
            df = self.data_prep[sheet]
        else:
            raise AttributeError("Read data preparation first.")

        pretty_df = self.skip_rows_df(df, header)
        df = pretty_df[pretty_df['REGION'] == region]
        input_otoole = self.input_otoole
        otoole_df = input_otoole[param]
        cols = otoole_df.columns
        register = df[set_label].values
        rows = (otoole_df[set_label].isin(register))
        rows = (rows) & (otoole_df['REGION'] == region)
        otoole_df_copy = otoole_df.copy()
        otoole_df_copy.loc[rows, :] = df[cols].values
        input_otoole[param] = otoole_df_copy

        self.input_otoole = input_otoole
        return input_otoole

    def add_segregable_param(
            self,
            sheet: str,
            param: str = "EmissionActivityRatio",
            region: str = "RE1",
            header: str = "EMISSION",
            set_label: str = "TECHNOLOGY"
    ) -> dict[pd.DataFrame]:
        """Update indices the parameter depends on.

        Where header is the category to group by
        while set_label the set column name to be filtered
        with unique values.
        """
        if hasattr(self, "data_prep"):
            df = self.data_prep[sheet]
        else:
            raise AttributeError("Read data preparation first.")

        pretty_df = self.skip_rows_df(df, header)
        dict_df = self.break_down_df(pretty_df, header, set_type=True)
        input_otoole = self.input_otoole
        otoole_df = input_otoole[param]
        for category, df in dict_df.items():
            set_codes = df[set_label].unique()
            rows = (otoole_df[set_label].isin(set_codes))
            rows = (rows) & (otoole_df[header] == category)
            rows = (rows) & (otoole_df['REGION'] == region)
            cols = otoole_df.columns
            otoole_df_copy = otoole_df.copy()
            otoole_df_copy[rows] = df[cols].values
            input_otoole[param] = otoole_df_copy

        self.input_otoole = input_otoole
        return input_otoole

    def add_emission_param(
            self,
            sheet: str,
            param: str = "EmissionActivityRatio",
            region: str = "RE1",
            header: str = "EMISSION",
            set_label: str = "TECHNOLOGY"
    ) -> dict[pd.DataFrame]:
        """Associate emission flows to technologies.

        If header is a set kind.
        """
        if hasattr(self, "data_prep"):
            df = self.data_prep[sheet]
        else:
            raise AttributeError("Read data preparation first.")

        pretty_df = self.skip_rows_df(df, header)
        dict_df = self.break_down_df(pretty_df, header, set_type=True)
        input_otoole = self.input_otoole
        otoole_df = input_otoole[param]
        for emission, df in dict_df.items():
            set_codes = df[set_label].unique()
            rows = (otoole_df[set_label].isin(set_codes))
            rows = (rows) & (otoole_df['EMISSION'] == emission)
            rows = (rows) & (otoole_df['REGION'] == region)
            cols = otoole_df.columns
            otoole_df_copy = otoole_df.copy()
            otoole_df_copy[rows] = df[cols].values
            input_otoole[param] = otoole_df_copy

        self.input_otoole = input_otoole
        return input_otoole

    def write_otoole_data(
            self,
            input_otoole: dict[pd.DataFrame]
    ) -> dict[pd.DataFrame]:
        """Override ``sandtool.xlsx`` file.

        Run untill fill-up with input data.
        """
        out_path = self.input_otoole_path
        short_n = self.set_short_names()
        with pd.ExcelWriter(out_path) as writer:
            for sheet, df in input_otoole.items():
                s_name = short_n[sheet]
                df.to_excel(writer, sheet_name=s_name, index=False)

        return input_otoole


if __name__ == "__main__":
    # Call clicSAND interface
    # -----------------------
    sand_data = Sand_Interface(2015,
                               2070,
                               config_path="./HandsOn/Trunk/config.yaml")
    input_sand = sand_data.set_input_data("./HandsOn/Trunk/InputSand.xlsm")
    sand_data.write_sand_config_file()

    # Call otoole interface
    # ---------------------
    otoole_data = Otoole_Interface(
        input_otoole_path="./HandsOn/Trunk/sandtool.xlsx",
        config_sand_path="./HandsOn/Trunk/sand_config.yaml"
    )
    input_otoole = otoole_data.populate_template(sand_data)
    _ = otoole_data.write_otoole_data(input_otoole)
