import os,sys,math,rasterio,yaml,datetime,numpy,geopandas,hf_hydrodata,pygeohydro,shapely

class Namelist:

    class DirectoryNames:
        project                 = ''
        inputs                  = ''
        wtd                     = ''
        wtd_parflow             = ''
        wtd_parflow_raw         = ''
        wtd_parflow_resampled   = ''
        wtd_fan                 = ''
        topo                    = ''
        dem                     = ''
        dem_breached            = ''
        twi                     = ''
        twi_mean                = ''
        slope                   = ''
        strm_mask               = ''
        facc                    = ''
        soils                   = ''
        domain                  = ''
        nhd                     = '' 
        pysda                   = ''
        output                  = ''
        output_raw              = ''
        output_summary          = ''

    class Time:
        start_date              = ''
        end_date                = ''
        datetime_dim            = ''

    class FileNames:
        namlistyaml             = ''
        domain                  = ''
        domain_mask             = ''
        nhd                     = ''
        dem                     = ''
        dem_user                = ''
        dem_breached            = ''
        soil_texture            = ''
        soil_transmissivity     = ''
        flow_acc                = ''
        facc_strm_mask          = ''
        slope                   = ''
        twi                     = ''
        twi_mean                = ''

    class Options:
        name_domain             = ''    
        overwrite_flag          = False
        verbose                 = False
        resample_method         = None
        name_resample_method    = ''
        huc_break_lvl           = ''
        facc_strm_threshold     = 0
        pp                      = False
        core_count              = ''

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
        self.dirnames.inputs                  = os.path.join(self.dirnames.project,     'inputs')
        self.dirnames.wtd                     = os.path.join(self.dirnames.inputs,      'wtd')
        self.dirnames.wtd_parflow             = os.path.join(self.dirnames.wtd,         'parflow')
        self.dirnames.wtd_parflow_raw         = os.path.join(self.dirnames.wtd_parflow, 'raw')
        self.dirnames.wtd_parflow_resampled   = os.path.join(self.dirnames.wtd_parflow, 'resampled')
        self.dirnames.wtd_fan                 = os.path.join(self.dirnames.wtd,         'fan')
        self.dirnames.topo                    = os.path.join(self.dirnames.inputs,      'topo')
        self.dirnames.soils                   = os.path.join(self.dirnames.inputs,      'soils')
        self.dirnames.domain                  = os.path.join(self.dirnames.inputs,      'domain')
        self.dirnames.nhd                     = os.path.join(self.dirnames.inputs,      'nhd')
        if not os.path.isdir(self.dirnames.output): # in case using user-specified output directory
            self.dirnames.output              = os.path.join(self.dirnames.project,     'outputs')
        self.dirnames.output_raw              = os.path.join(self.dirnames.output,      'raw')
        self.dirnames.output_summary          = os.path.join(self.dirnames.output,      'summary')

    def _set_f_names(self):
        self.fnames.domain                    = os.path.join(self.dirnames.domain,      'domain.gpkg')
        self.fnames.domain_mask               = os.path.join(self.dirnames.domain,      'domain_mask.tiff')
        self.fnames.nhd                       = os.path.join(self.dirnames.nhd,         'nhd_hr.gpkg')
        self.fnames.dem                       = os.path.join(self.dirnames.topo,        'dem.tiff')
        self.fnames.dem_breached              = os.path.join(self.dirnames.topo,        'dem_breached.tiff')
        self.fnames.soil_texture              = os.path.join(self.dirnames.soils,       'soil_texture.gpkg')
        self.fnames.soil_transmissivity       = os.path.join(self.dirnames.soils,       'soil_transmissivity.tiff')
        self.fnames.flow_acc                  = os.path.join(self.dirnames.topo,        'flow_acc.tiff')
        self.fnames.slope                     = os.path.join(self.dirnames.topo,        'slope.tiff')
        self.fnames.facc_strm_mask            = os.path.join(self.dirnames.topo,        'facc_strm_mask.tiff')
        self.fnames.twi                       = os.path.join(self.dirnames.topo,        'twi.tiff')
        self.fnames.twi_mean                  = os.path.join(self.dirnames.topo,        'twi_mean.tiff')

    def _make_dirs(self):
        """Make subdirectory structure"""
        for d in self.dirnames.__dict__:
            if not os.path.isdir(self.dirnames.__dict__[d]):
                os.makedirs(self.dirnames.__dict__[d])

    def _read_inputyaml(self,fname:str):
        self.fnames.namlistyaml = fname
        with open(self.fnames.namlistyaml,'r') as yamlf:
            try: 
                return yaml.safe_load(yamlf)
            except yaml.YAMLError as exc: 
                print(exc)

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
        name_var = 'output_directory'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        if not os.path.isdir(userinput[name_var]):
            try:
                os.mkdir(userinput[name_var])
            except OSError as e:
                sys.exit(f'ERROR could not create project directory {userinput[name_var]}: {e}')
        self.dirnames.output = os.path.abspath(userinput[name_var])
        #
        #
        name_var = 'domain_name'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        self.options.name_domain = userinput[name_var]
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
        name_pysda = 'pysda'
        if name_pysda not in userinput:
            sys.exit(f'ERROR required variable {name_pysda} not found {fname_yaml_input}')
        if not os.path.isdir(userinput[name_pysda]):
            sys.exit(f'ERROR pysda directory {userinput[name_pysda]} does not exist')
        if not os.path.isfile(os.path.join(userinput[name_pysda],'sdapoly.py')): 
            sys.exit(f'ERROR pysda directory {userinput[name_pysda]} does not contain sdapoly.py')
        if not os.path.isfile(os.path.join(userinput[name_pysda],'sdaprop.py')): 
            sys.exit(f'ERROR pysda directory {userinput[name_pysda]} does not contain sdaprop.py')
        self.dirnames.pysda = os.path.abspath(userinput[name_pysda])
        if self.dirnames.pysda not in sys.path: sys.path.append(self.dirnames.pysda)
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
            self.options.overwrite_flag = True
        else:
            self.options.overwrite_flag = False
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
        #
        #
        if isinstance(self.options.huc_break_lvl, int) or isinstance(self.options.core_count):
            self.options.pp = True
        else:
            self.options.pp = False

    def get_subdomain_fnames(self):
        if namelist.options.verbose: print('calling _set_subdomain_comp_units')
        dtfnames = {'fname_subdomain'           : [namelist.dirnames.domain,'subdomain.gpkg'],
                    'fname_subdomain_mask'      : [namelist.dirnames.domain,'subdomain_mask.tiff'],
                    'fname_dem'                 : [namelist.dirnames.topo,namelist.fnames.dem],
                    'fname_dem_breached'        : [namelist.dirnames.topo,namelist.fnames.dem_breached],
                    'fname_facc'                : [namelist.dirnames.topo,namelist.fnames.flow_acc],
                    'fname_strm_mask'           : [namelist.dirnames.topo,namelist.fnames.facc_strm_mask],
                    'fname_slope'               : [namelist.dirnames.topo,namelist.fnames.slope],
                    'fname_twi'                 : [namelist.dirnames.topo,namelist.fnames.twi],
                    'fname_twi_mean'            : [namelist.dirnames.topo,namelist.fnames.twi_mean],
                    'fname_soil_texture'        : [namelist.dirnames.soils,namelist.fnames.soil_texture],
                    'fname_soil_transmissivity' : [namelist.dirnames.soils,namelist.fnames.soil_transmissivity],
                    'fname_nhd'                 : [namelist.dirnames.nhd,namelist.fnames.nhd]}
        dtdnames = {'dirname_wtd_raw'           : namelist.dirnames.wtd_parflow_raw,
                    'dirname_wtd_reprj_resmple' : namelist.dirnames.wtd_parflow_resampled,
                    'dirname_wtd_output_raw'    : namelist.dirnames.output_raw,
                    'dirname_wtd_output_summary': namelist.dirnames.output_summary}
        if not os.path.isfile(namelist.fnames.hucs) or namelist.options.overwrite_flag:
            brk_lvl = namelist.pp.huc_break_lvl
            if not isinstance(brk_lvl,int): 
                brk_lvl = len(str(namelist.vars.huc))