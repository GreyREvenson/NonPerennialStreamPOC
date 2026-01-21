import os,sys,math,rasterio,yaml,datetime,numpy,geopandas,hf_hydrodata,pygeohydro,shapely

class Namelist:

    class DirectoryNames:
        project                 = ''
        input                   = ''
        output                  = ''

    class Time:
        start_date              = ''
        end_date                = ''
        datetime_dim            = ''

    class FileNames:
        domain                  = ''
        dem_user                = ''

    class Options:
        domain_hucid            = ''    
        domain_bbox             = ''
        domain_latlon           = ''
        overwrite               = False
        verbose                 = False
        resample_method         = None
        name_resample_method    = ''
        huc_break_lvl           = ''
        facc_strm_threshold     = 0
        pp                      = False
        core_count              = ''
        write_resampled_wtd     = False

    def __init__(self,filename:str):
        self._init_vars()
        self._set_user_inputs(filename)
        self._set_names()
        self._make_dirs()

    def _init_vars(self):
        self.dirnames = Namelist.DirectoryNames()
        self.fnames   = Namelist.FileNames()
        self.options  = Namelist.Options()
        self.time     = Namelist.Time()

    def _set_names(self):
        self._set_d_names()
        self._set_f_names()

    def _set_d_names(self):
        self.dirnames.input             = os.path.join(self.dirnames.project, 'input')
        self.dirnames.output            = os.path.join(self.dirnames.project, 'output')

    def _set_f_names(self):
        self.fnames.domain              = os.path.join(self.dirnames.input, 'domain.gpkg')

    def _make_dirs(self):
        for d in self.dirnames.__dict__:
            os.makedirs(self.dirnames.__dict__[d],exist_ok=True)

    def _read_inputyaml(self,fname:str):
        self.fnames.namlistyaml = fname
        with open(self.fnames.namlistyaml,'r') as yamlf:
            try: 
                return yaml.safe_load(yamlf)
            except yaml.YAMLError as yerr: 
                print(yerr)

    def _set_user_inputs(self,fname_yaml_input:str):
        """Set variables using read-in values"""
        #
        #
        userinput = self._read_inputyaml(fname_yaml_input)
        #
        #
        name_var = 'project_directory'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        if not os.path.isdir(userinput[name_var]):
            try:
                os.mkdir(userinput[name_var])
            except OSError as e:
                sys.exit(f'ERROR could not create project directory {userinput[name_var]}: {e}')
        self.dirnames.project = os.path.abspath(userinput[name_var])
        #
        #
        names_domain = ['domain_huc','domain_latlon','domain_bbox']
        if len([name for name in names_domain if name in userinput]) == 0:
            sys.exit(f'ERROR at least one of the required domain variables ({', '.join(names_domain)}) not found {fname_yaml_input}')
        name_var = 'domain_huc'
        if name_var in userinput:
            self.options.domain_hucid = userinput[name_var]
        name_var = 'domain_latlon'
        if name_var in userinput:
            self.options.domain_latlon = userinput[name_var]
        name_var = 'domain_bbox'
        if name_var in userinput:
            self.options.domain_bbox = userinput[name_var]
        #
        #
        name_var = 'start_date'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        try:
            dt = userinput[name_var].split('-')
            dt = datetime.datetime(year=int(dt[0]),month=int(dt[1]),day=int(dt[2]))
            self.time.start_date = dt
        except ValueError:
            sys.exit(f'ERROR invalid start date {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'end_date'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        try:
            dt = userinput[name_var].split('-')
            dt = datetime.datetime(year=int(dt[0]),month=int(dt[1]),day=int(dt[2]))
            self.time.end_date = dt
        except ValueError:
            sys.exit(f'ERROR invalid start date {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        dt_dim = list()
        idt = self.time.start_date
        while idt <= self.time.end_date:
            dt_dim.append(idt)
            idt += datetime.timedelta(days=1)
        self.time.datetime_dim = numpy.array(dt_dim)
        #
        #
        name_var = 'facc_strm_threshold'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        try:
            self.options.facc_strm_threshold = int(userinput[name_var])
        except ValueError:
            sys.exit(f'ERROR invalid start date {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'dem'
        if name_var in userinput:
            if not os.path.isfile(os.path.abspath(userinput[name_var])):
                sys.exit(f'ERROR invalid dem input file {userinput[name_var]} in {fname_yaml_input}')
            self.fnames.dem_user = os.path.abspath(userinput[name_var])
        #
        #
        name_var = 'overwrite'
        if name_var in userinput and str(userinput[name_var]).upper().find('TRUE') != -1:
            self.options.overwrite = True
        else:
            self.options.overwrite = False
        #
        #
        name_var = 'verbose'
        if name_var in userinput and str(userinput[name_var]).upper().find('TRUE') != -1:
            self.options.verbose = True
        else:
            self.options.verbose = False
        #
        #
        name_var = 'wtd_resample_method'
        if name_var in userinput:
            if str(userinput[name_var]).lower().find('bilinear') != -1:
                self.options.resample_method = rasterio.enums.Resampling.bilinear
                self.options.name_resample_method = 'bilinear'
            elif str(userinput[name_var]).lower().find('cubic') != -1:
                self.options.resample_method = rasterio.enums.Resampling.cubic
                self.options.name_resample_method = 'cubic'
            elif str(userinput[name_var]).lower().find('nearest') != -1:
                self.options.resample_method = rasterio.enums.Resampling.nearest
                self.options.name_resample_method = 'nearest'
            else:
                sys.exit(f'ERROR invalid wtd resample method {userinput[name_var]} in {fname_yaml_input}')
        else:
            self.options.resample_method = rasterio.enums.Resampling.bilinear
            self.options.name_resample_method = 'bilinear'
        #
        #
        name_var = 'pp_core_count'
        if name_var in userinput:
            try:
                self.options.core_count = int(userinput[name_var])
                self.options.pp         = True
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'pp_huc_break_lvl'
        if name_var in userinput:
            try:
                self.options.huc_break_lvl = int(userinput[name_var])
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')
        if name_var not in userinput and isinstance(self.options.core_count,int):
            sys.exit(f'ERROR {name_var} must be defined if pp_core_count is defined in {fname_yaml_input}')