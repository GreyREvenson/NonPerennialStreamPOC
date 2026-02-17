import os,sys,rasterio,yaml,datetime,numpy

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
        domain_hucid            = None    
        domain_bbox             = None
        domain_latlon           = None
        overwrite               = False
        verbose                 = False
        resample_method         = None
        name_resample_method    = 'bilinear'
        huc_lvl                 = None
        facc_strm_threshold     = None
        pp                      = False
        core_count              = None
        write_wtd_mean_resampled = False
        hf_hydrodata_un         = None
        hf_hydrodata_pin        = None
        dem_rez                 = None

    def __init__(self,filename:str):
        self._init_vars()
        self._set_user_inputs(filename)
        self._set_names()

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
        self.dirnames.project = os.path.dirname(os.path.abspath(fname_yaml_input))
        #print(f'project directory set to: {self.dirnames.project}')
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
        name_var = 'huc_lvl'
        if name_var in userinput:
            try:
                self.options.huc_lvl = int(userinput[name_var])
                if self.options.huc_lvl not in (2,4,6,8,10,12):
                    sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')
        if name_var not in userinput and isinstance(self.options.core_count,int):
            if self.options.verbose:
                print(f'INFO {name_var} not defined but pp_core_count is {self.options.core_count} : parallel processing will not be implemented')
        #
        #
        name_var = 'hf_hydrodata_un'
        if name_var in userinput:
            try:
                self.options.hf_hydrodata_un = userinput[name_var]
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'hf_hydrodata_pin'
        if name_var in userinput:
            try:
                self.options.hf_hydrodata_pin = userinput[name_var]
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'write_wtd_mean_resampled'
        if name_var in userinput and str(userinput[name_var]).upper().find('TRUE') != -1:
            self.options.write_wtd_mean_resampled = True
        else:
            self.options.write_wtd_mean_resampled = False
        #
        #
        name_var = 'dem_rez'
        if name_var in userinput:
            try:
                self.options.dem_rez = float(userinput[name_var])
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')