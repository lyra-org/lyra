// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use ::darling::FromMeta;
use ::proc_macro::TokenStream;
use ::syn::{
    Result,
    TypePath,
};

#[derive(Debug, FromMeta)]
#[darling(derive_syn_parse)]
pub struct CompileArgs {
    pub type_path: TypePath,
    #[darling(default)]
    pub fields: Option<bool>,
    #[darling(default)]
    pub methods: Option<bool>,
    #[darling(default)]
    pub variants: Option<bool>,
}

pub fn parse_compile_args(input: TokenStream) -> Result<CompileArgs> {
    syn::parse(input)
}

#[cfg(test)]
mod tests {
    use super::CompileArgs;

    #[test]
    fn compile_args_require_type_path() {
        let error = syn::parse2::<CompileArgs>(quote::quote!(fields = true)).unwrap_err();

        assert!(error.to_string().contains("type_path"));
    }
}
