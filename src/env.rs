#[derive(Debug, PartialEq, Eq)]
pub enum EnvError {
    NotPresent,
    ParseError(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct StrVar(&'static str, Option<&'static str>);

impl StrVar {
    pub fn get(&self) -> Result<String, EnvError> {
        match get_env(self.0) {
            Err(EnvError::NotPresent) => {
                if let Some(default) = self.1 {
                    Ok(default.to_owned())
                } else {
                    Err(EnvError::NotPresent)
                }
            }
            x => x,
        }
    }
}

#[macro_export]
macro_rules! define_str_var {
    ($var_name: ident, $env_name: expr) => {
        pub const $var_name: StrVar = StrVar($env_name, None);
    };
    ($var_name: ident, $env_name: expr, $default: expr) => {
        pub const $var_name: StrVar = StrVar($env_name, Some($default));
    };
}

macro_rules! define_var {
    ($var_name: ident, $typ: ty) => {
        #[derive(Debug)]
        pub struct $var_name(&'static str, Option<$typ>);
        
        impl $var_name {
            pub fn get(&self) -> Result<$typ, EnvError> {
                match get_env(self.0) {
                    Ok(v) => {
                        match v.parse() {
                            Ok(v) => Ok(v),
                            Err(err) => {
                                Err(EnvError::ParseError(format!("{}", err)))
                            }
                        }
                    }
                    Err(err @ EnvError::ParseError(_)) => {
                        Err(err)
                    }
                    Err(EnvError::NotPresent) => {
                        if let Some(default) = self.1 {
                            Ok(default.to_owned())
                        } else {
                            Err(EnvError::NotPresent)
                        }
                    }
                }
            }
        }
    };
}

define_var!(I64Var, i64);

#[macro_export]
macro_rules! define_i64_var {
    ($var_name: ident, $env_name: expr) => {
        pub const $var_name: I64Var = I64Var($env_name, None);
    };
    ($var_name: ident, $env_name: expr, $default: expr) => {
        pub const $var_name: I64Var = I64Var($env_name, Some($default));
    };
}

define_var!(F64Var, f64);

#[macro_export]
macro_rules! define_f64_var {
    ($var_name: ident, $env_name: expr) => {
        pub const $var_name: F64Var = F64Var($env_name, None);
    };
    ($var_name: ident, $env_name: expr, $default: expr) => {
        pub const $var_name: F64Var = F64Var($env_name, Some($default));
    };
}


fn get_env(var_name: &str) -> Result<String, EnvError> {
    match std::env::var(var_name) {
        Ok(v) => Ok(v),
        Err(std::env::VarError::NotPresent) => Err(EnvError::NotPresent),
        Err(err @ std::env::VarError::NotUnicode(_)) => {
            Err(EnvError::ParseError(format!("{}", err)))
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_env_not_present() {
        define_str_var!(VAR, "TEST_STR_ENV_NOT_PRESENT");
        assert_eq!(VAR.get(), Err(EnvError::NotPresent));
    }

    #[test]
    fn test_str_env() {
        std::env::set_var("TEST_STR_ENV", "ABC");
        define_str_var!(VAR, "TEST_STR_ENV");
        assert_eq!(VAR.get(), Ok("ABC".to_string()));
    }

    #[test]
    fn test_str_env_default() {
        define_str_var!(VAR, "TEST_STR_ENV_DEFAULT", "default");
        assert_eq!(VAR.get(), Ok("default".to_string()));
        std::env::set_var("TEST_STR_ENV_DEFAULT", "ABC");
        assert_eq!(VAR.get(), Ok("ABC".to_string()));
    }

    #[test]
    fn test_int_env() {
        std::env::set_var("TEST_INT_ENV", "123");
        define_i64_var!(VAR, "TEST_INT_ENV");
        assert_eq!(VAR.get(), Ok(123));
    }

    #[test]
    fn test_int_env_default() {
        define_i64_var!(VAR, "TEST_INT_ENV_DEFAULT", 0);
        assert_eq!(VAR.get(), Ok(0));
        std::env::set_var("TEST_INT_ENV_DEFAULT", "123");
        assert_eq!(VAR.get(), Ok(123));
    }

    #[test]
    fn test_int_env_not_present() {
        define_str_var!(VAR, "TEST_INT_ENV_NOT_PRESENT");
        assert_eq!(VAR.get(), Err(EnvError::NotPresent));
    }

    #[test]
    fn test_int_env_parse_err() {
        std::env::set_var("TEST_INT_ENV_PARSE_ERR", "a");
        define_i64_var!(VAR, "TEST_INT_ENV_PARSE_ERR");
        match VAR.get() {
            Err(EnvError::ParseError(_)) => {}
            x => {
                panic!("{:?}", x);
            }
        }
    }

    #[test]
    fn test_fp_env() {
        std::env::set_var("TEST_FP_ENV", "123");
        define_f64_var!(VAR, "TEST_FP_ENV");
        assert_eq!(VAR.get(), Ok(123.0));
    }

    #[test]
    fn test_fp_env_default() {
        define_f64_var!(VAR, "TEST_FP_ENV_DEFAULT", 0.0);
        assert_eq!(VAR.get(), Ok(0.0));
        std::env::set_var("TEST_FP_ENV_DEFAULT", "123");
        assert_eq!(VAR.get(), Ok(123.0));
    }

    #[test]
    fn test_fp_env_not_present() {
        define_str_var!(VAR, "TEST_FP_ENV_NOT_PRESENT");
        assert_eq!(VAR.get(), Err(EnvError::NotPresent));
    }

    #[test]
    fn test_fp_env_parse_err() {
        std::env::set_var("TEST_FP_ENV_PARSE_ERR", "a");
        define_f64_var!(VAR, "TEST_FP_ENV_PARSE_ERR");
        match VAR.get() {
            Err(EnvError::ParseError(_)) => {}
            x => {
                panic!("{:?}", x);
            }
        }
    }
}
