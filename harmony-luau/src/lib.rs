// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};
use std::fmt::{
    self,
    Display,
    Write,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LuauType {
    Literal(&'static str),
    Optional(Box<LuauType>),
    Array(Box<LuauType>),
    Map {
        key: Box<LuauType>,
        value: Box<LuauType>,
    },
    Function(FunctionType),
    Union(Vec<LuauType>),
}

impl LuauType {
    pub const fn literal(value: &'static str) -> Self {
        Self::Literal(value)
    }

    pub const fn any() -> Self {
        Self::Literal("any")
    }

    pub const fn thread() -> Self {
        Self::Literal("thread")
    }

    pub fn optional(inner: LuauType) -> Self {
        match inner {
            Self::Optional(inner) => Self::Optional(inner),
            other => Self::Optional(Box::new(other)),
        }
    }

    pub fn array(inner: LuauType) -> Self {
        Self::Array(Box::new(inner))
    }

    pub fn map(key: LuauType, value: LuauType) -> Self {
        Self::Map {
            key: Box::new(key),
            value: Box::new(value),
        }
    }

    pub fn function(params: Vec<FunctionParameter>, returns: Vec<LuauType>) -> Self {
        Self::Function(FunctionType { params, returns })
    }

    pub fn union(types: Vec<LuauType>) -> Self {
        Self::Union(types)
    }
}

impl Display for LuauType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(value) => write!(f, "{value}"),
            Self::Optional(inner) => {
                if matches!(
                    inner.as_ref(),
                    Self::Literal(_) | Self::Array(_) | Self::Map { .. }
                ) {
                    write!(f, "{inner}?")
                } else {
                    write!(f, "({inner})?")
                }
            }
            Self::Array(inner) => write!(f, "{{{inner}}}"),
            Self::Map { key, value } => write!(f, "{{ [{key}]: {value} }}"),
            Self::Function(signature) => write!(f, "{signature}"),
            Self::Union(types) => {
                for (index, ty) in types.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" | ")?;
                    }
                    write!(f, "{}", render_union_member(ty))?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionType {
    pub params: Vec<FunctionParameter>,
    pub returns: Vec<LuauType>,
}

impl Display for FunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(")?;
        for (index, param) in self.params.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            if param.variadic {
                write!(f, "{}", render_variadic_type(&param.ty))?;
            } else if let Some(name) = param.name {
                write!(f, "{name}: {}", param.ty)?;
            } else {
                write!(f, "{}", param.ty)?;
            }
        }
        f.write_str(") -> ")?;
        write!(f, "{}", render_return_types(&self.returns))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionParameter {
    pub name: Option<&'static str>,
    pub ty: LuauType,
    pub variadic: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParameterDescriptor {
    pub name: &'static str,
    pub ty: LuauType,
    pub description: Option<&'static str>,
    pub variadic: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldDescriptor {
    pub name: &'static str,
    pub ty: LuauType,
    pub description: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MethodKind {
    Static,
    Instance,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MethodDescriptor {
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub params: Vec<ParameterDescriptor>,
    pub returns: Vec<LuauType>,
    pub yields: bool,
    pub kind: MethodKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClassDescriptor {
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub fields: Vec<FieldDescriptor>,
    pub methods: Vec<MethodDescriptor>,
}

impl ClassDescriptor {
    pub fn new(name: &'static str, description: Option<&'static str>) -> Self {
        Self {
            name,
            description,
            fields: Vec::new(),
            methods: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleFunctionDescriptor {
    pub path: Vec<&'static str>,
    pub description: Option<&'static str>,
    pub params: Vec<ParameterDescriptor>,
    pub returns: Vec<LuauType>,
    pub yields: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalFunctionDescriptor {
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub params: Vec<ParameterDescriptor>,
    pub returns: Vec<LuauType>,
    pub yields: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleDescriptor {
    pub name: &'static str,
    pub local_name: &'static str,
    pub description: Option<&'static str>,
    pub functions: Vec<ModuleFunctionDescriptor>,
}

impl ModuleDescriptor {
    pub fn new(
        name: &'static str,
        local_name: &'static str,
        description: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            local_name,
            description,
            functions: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeAliasDescriptor {
    pub name: &'static str,
    pub ty: LuauType,
    pub description: Option<&'static str>,
}

impl TypeAliasDescriptor {
    pub fn new(name: &'static str, ty: LuauType, description: Option<&'static str>) -> Self {
        Self {
            name,
            ty,
            description,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterfaceDescriptor {
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub fields: Vec<FieldDescriptor>,
}

impl InterfaceDescriptor {
    pub fn new(name: &'static str, description: Option<&'static str>) -> Self {
        Self {
            name,
            description,
            fields: Vec::new(),
        }
    }
}

pub trait LuauTypeInfo {
    fn luau_type() -> LuauType;
}

pub trait LuauReturn {
    fn luau_returns() -> Vec<LuauType>;
}

pub trait DescribeUserData {
    fn class_descriptor() -> ClassDescriptor;
}

pub trait DescribeInterface {
    fn interface_descriptor() -> InterfaceDescriptor;
}

pub trait DescribeTypeAlias {
    fn type_alias_descriptor() -> TypeAliasDescriptor;
}

pub trait DescribeModule {
    fn module_descriptor() -> ModuleDescriptor;
}

pub struct JsonValue;

impl LuauReturn for () {
    fn luau_returns() -> Vec<LuauType> {
        Vec::new()
    }
}

impl<T> LuauReturn for T
where
    T: LuauTypeInfo,
{
    fn luau_returns() -> Vec<LuauType> {
        vec![T::luau_type()]
    }
}

impl LuauTypeInfo for JsonValue {
    fn luau_type() -> LuauType {
        LuauType::literal("JsonValue")
    }
}

impl DescribeTypeAlias for JsonValue {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "JsonValue",
            LuauType::optional(LuauType::union(vec![
                LuauType::literal("boolean"),
                LuauType::literal("number"),
                LuauType::literal("string"),
                LuauType::array(LuauType::literal("JsonValue")),
                LuauType::map(LuauType::literal("string"), LuauType::literal("JsonValue")),
            ])),
            Some("JSON-compatible value."),
        )
    }
}

macro_rules! impl_tuple_returns {
    ($(($($name:ident),+)),+ $(,)?) => {
        $(
            impl<$($name),+> LuauReturn for ($($name,)+)
            where
                $($name: LuauTypeInfo),+
            {
                fn luau_returns() -> Vec<LuauType> {
                    vec![$($name::luau_type()),+]
                }
            }
        )+
    };
}

impl_tuple_returns! {
    (A, B),
    (A, B, C),
    (A, B, C, D)
}

impl<T, E> LuauReturn for Result<T, E>
where
    T: LuauReturn,
{
    fn luau_returns() -> Vec<LuauType> {
        T::luau_returns()
    }
}

macro_rules! impl_literal_types {
    ($literal:literal => $($ty:ty),+ $(,)?) => {
        $(
            impl LuauTypeInfo for $ty {
                fn luau_type() -> LuauType {
                    LuauType::literal($literal)
                }
            }
        )+
    };
}

impl_literal_types!("boolean" => bool);
impl_literal_types!("number" => i8, i16, i32, i64, i128, isize);
impl_literal_types!("number" => u8, u16, u32, u64, u128, usize);
impl_literal_types!("number" => f32, f64);
impl_literal_types!("string" => String, &'static str, char);
impl_literal_types!("string" => std::path::PathBuf);

impl<T> LuauTypeInfo for Option<T>
where
    T: LuauTypeInfo,
{
    fn luau_type() -> LuauType {
        LuauType::optional(T::luau_type())
    }
}

impl<T> LuauTypeInfo for Vec<T>
where
    T: LuauTypeInfo,
{
    fn luau_type() -> LuauType {
        LuauType::array(T::luau_type())
    }
}

impl<T, const N: usize> LuauTypeInfo for [T; N]
where
    T: LuauTypeInfo,
{
    fn luau_type() -> LuauType {
        LuauType::array(T::luau_type())
    }
}

impl<T> LuauTypeInfo for Box<T>
where
    T: LuauTypeInfo,
{
    fn luau_type() -> LuauType {
        T::luau_type()
    }
}

impl<K, V, S> LuauTypeInfo for HashMap<K, V, S>
where
    K: LuauTypeInfo,
    V: LuauTypeInfo,
    S: std::hash::BuildHasher,
{
    fn luau_type() -> LuauType {
        LuauType::map(K::luau_type(), V::luau_type())
    }
}

impl<K, V> LuauTypeInfo for BTreeMap<K, V>
where
    K: LuauTypeInfo,
    V: LuauTypeInfo,
{
    fn luau_type() -> LuauType {
        LuauType::map(K::luau_type(), V::luau_type())
    }
}

pub fn render_definition_file(
    module: &ModuleDescriptor,
    classes: &[ClassDescriptor],
) -> Result<String, fmt::Error> {
    render_definition_file_with_support(module, &[], &[], classes)
}

pub fn render_definition_file_with_support(
    module: &ModuleDescriptor,
    aliases: &[TypeAliasDescriptor],
    interfaces: &[InterfaceDescriptor],
    classes: &[ClassDescriptor],
) -> Result<String, fmt::Error> {
    let mut output = String::new();

    for alias in aliases {
        render_alias_definition(&mut output, module.name, alias)?;
        output.push('\n');
    }

    for interface in interfaces {
        render_interface_definition(&mut output, module.name, interface)?;
        output.push('\n');
    }

    for (index, class) in classes.iter().enumerate() {
        if index > 0 || !aliases.is_empty() || !interfaces.is_empty() {
            output.push('\n');
        }
        render_class_definition(&mut output, class)?;
        output.push('\n');
    }

    if !classes.is_empty() || !aliases.is_empty() || !interfaces.is_empty() {
        output.push('\n');
    }

    render_module_definition(&mut output, module, classes)?;
    Ok(output)
}

pub fn render_globals_definition(
    globals: &[GlobalFunctionDescriptor],
) -> Result<String, fmt::Error> {
    let mut output = String::new();
    for (index, global) in globals.iter().enumerate() {
        if index > 0 {
            output.push('\n');
        }
        render_global_definition(&mut output, global)?;
    }
    Ok(output)
}

fn render_global_definition(output: &mut String, global: &GlobalFunctionDescriptor) -> fmt::Result {
    let mut lines = vec![format!("@function {}", global.name)];
    if global.yields {
        lines.push("@yields".to_string());
    }
    push_description(&mut lines, global.description);
    render_doc_block(output, &lines)?;
    writeln!(
        output,
        "declare function {}({}){}",
        global.name,
        render_signature_params(&global.params),
        render_return_annotation(&global.returns),
    )
}

fn is_enum_class(class: &ClassDescriptor) -> bool {
    if class.methods.is_empty() && !class.fields.is_empty() {
        class
            .fields
            .iter()
            .all(|field| matches!(&field.ty, LuauType::Literal(lit) if *lit == class.name))
    } else {
        false
    }
}

fn render_alias_definition(
    output: &mut String,
    module_name: &str,
    alias: &TypeAliasDescriptor,
) -> fmt::Result {
    let mut lines = vec![
        format!("@type {} {}", alias.name, alias.ty),
        format!("@within {module_name}"),
    ];
    push_description(&mut lines, alias.description);
    render_doc_block(output, &lines)?;
    writeln!(output, "export type {} = {}", alias.name, alias.ty)
}

fn render_interface_definition(
    output: &mut String,
    module_name: &str,
    interface: &InterfaceDescriptor,
) -> fmt::Result {
    let mut lines = vec![
        format!("@interface {}", interface.name),
        format!("@within {module_name}"),
    ];
    push_description(&mut lines, interface.description);
    for field in &interface.fields {
        let line = match field.description {
            Some(description) => format!(".{} {} -- {}", field.name, field.ty, description),
            None => format!(".{} {}", field.name, field.ty),
        };
        lines.push(line);
    }
    render_doc_block(output, &lines)?;
    writeln!(output, "export type {} = {{", interface.name)?;
    for field in &interface.fields {
        writeln!(output, "\t{}: {},", field.name, field.ty)?;
    }
    writeln!(output, "}}")
}

fn render_class_definition(output: &mut String, class: &ClassDescriptor) -> fmt::Result {
    let mut interface_lines = vec![format!("@interface {}", class.name)];
    push_description(&mut interface_lines, class.description);
    render_doc_block(output, &interface_lines)?;
    writeln!(output, "export type {} = {{", class.name)?;
    for field in &class.fields {
        let mut lines = Vec::new();
        push_description(&mut lines, field.description);
        render_indented_doc_block(output, "\t", &lines)?;
        writeln!(output, "\t{}: {},", field.name, field.ty)?;
    }

    for method in class
        .methods
        .iter()
        .filter(|method| method.kind == MethodKind::Instance)
    {
        let mut lines = Vec::new();
        if method.yields {
            lines.push("@yields".to_string());
        }
        push_description(&mut lines, method.description);
        render_indented_doc_block(output, "\t", &lines)?;
        let ty = LuauType::function(
            std::iter::once(FunctionParameter {
                name: Some("self"),
                ty: LuauType::literal(class.name),
                variadic: false,
            })
            .chain(method.params.iter().map(|param| FunctionParameter {
                name: Some(param.name),
                ty: param.ty.clone(),
                variadic: param.variadic,
            }))
            .collect(),
            method.returns.clone(),
        );
        writeln!(output, "\t{}: {},", method.name, ty)?;
    }
    writeln!(output, "}}")?;
    writeln!(output)?;

    let static_methods = class
        .methods
        .iter()
        .filter(|method| method.kind == MethodKind::Static)
        .collect::<Vec<_>>();

    if static_methods.is_empty() {
        return Ok(());
    }

    let mut class_lines = vec![format!("@class {}", class.name)];
    push_description(&mut class_lines, class.description);
    render_doc_block(output, &class_lines)?;
    writeln!(output, "local {} = {{}}", class.name)?;
    writeln!(output)?;

    for method in static_methods {
        let mut lines = vec![format!("@within {}", class.name)];
        if method.yields {
            lines.push("@yields".to_string());
        }
        push_description(&mut lines, method.description);
        render_doc_block(output, &lines)?;

        writeln!(
            output,
            "function {}.{}({}){}",
            class.name,
            method.name,
            render_signature_params(&method.params),
            render_return_annotation(&method.returns),
        )?;
        writeln!(output, "\treturn nil :: any")?;
        writeln!(output, "end")?;
        writeln!(output)?;
    }

    Ok(())
}

fn render_module_definition(
    output: &mut String,
    module: &ModuleDescriptor,
    classes: &[ClassDescriptor],
) -> fmt::Result {
    let mut lines = vec![format!("@class {}", module.name)];
    push_description(&mut lines, module.description);
    render_doc_block(output, &lines)?;
    writeln!(output, "local {} = {{}}", module.local_name)?;

    let enum_classes: Vec<&ClassDescriptor> = classes.iter().filter(|c| is_enum_class(c)).collect();
    if !enum_classes.is_empty() {
        writeln!(output)?;
        for class in enum_classes {
            writeln!(
                output,
                "{}.{} = nil :: {}",
                module.local_name, class.name, class.name,
            )?;
        }
    }

    let nested_tables = collect_nested_tables(module);
    if !nested_tables.is_empty() {
        writeln!(output)?;
        for table_path in nested_tables {
            writeln!(
                output,
                "{}.{} = {{}}",
                module.local_name,
                table_path.join("."),
            )?;
        }
    }

    if !module.functions.is_empty() {
        writeln!(output)?;
    }

    for function in &module.functions {
        let mut lines = vec![format!("@within {}", module.name)];
        if function.yields {
            lines.push("@yields".to_string());
        }
        push_description(&mut lines, function.description);
        render_doc_block(output, &lines)?;
        writeln!(
            output,
            "function {}.{}({}){}",
            module.local_name,
            function.path.join("."),
            render_signature_params(&function.params),
            render_return_annotation(&function.returns),
        )?;
        writeln!(output, "\treturn nil :: any")?;
        writeln!(output, "end")?;
        writeln!(output)?;
    }

    writeln!(output, "return {}", module.local_name)?;
    Ok(())
}

fn render_doc_block(output: &mut String, lines: &[String]) -> fmt::Result {
    render_doc_block_with_indent(output, "", lines)
}

fn render_indented_doc_block(output: &mut String, indent: &str, lines: &[String]) -> fmt::Result {
    if lines.is_empty() {
        return Ok(());
    }

    render_doc_block_with_indent(output, indent, lines)
}

fn render_doc_block_with_indent(
    output: &mut String,
    indent: &str,
    lines: &[String],
) -> fmt::Result {
    writeln!(output, "{indent}--[=[")?;
    for line in lines {
        if line.is_empty() {
            writeln!(output, "{indent}")?;
        } else {
            writeln!(output, "{indent}\t{line}")?;
        }
    }
    writeln!(output, "{indent}]=]")
}

fn push_description(lines: &mut Vec<String>, description: Option<&'static str>) {
    let Some(description) = description else {
        return;
    };

    if !description.trim().is_empty() {
        lines.push(String::new());
        for line in description.lines() {
            lines.push(line.to_string());
        }
    }
}

fn render_signature_params(params: &[ParameterDescriptor]) -> String {
    params
        .iter()
        .map(render_signature_param)
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_signature_param(param: &ParameterDescriptor) -> String {
    if param.variadic {
        format!("...: {}", param.ty)
    } else {
        format!("{}: {}", param.name, param.ty)
    }
}

fn render_return_types(returns: &[LuauType]) -> String {
    match returns {
        [] => "()".to_string(),
        [value] => value.to_string(),
        values => {
            let joined = values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            format!("({joined})")
        }
    }
}

fn render_return_annotation(returns: &[LuauType]) -> String {
    if returns.is_empty() {
        String::new()
    } else {
        format!(": {}", render_return_types(returns))
    }
}

fn render_variadic_type(ty: &LuauType) -> String {
    if matches!(
        ty,
        LuauType::Literal(_) | LuauType::Array(_) | LuauType::Map { .. }
    ) {
        format!("...{ty}")
    } else {
        format!("...({ty})")
    }
}

fn render_union_member(ty: &LuauType) -> String {
    if matches!(ty, LuauType::Function(_)) {
        format!("({ty})")
    } else {
        ty.to_string()
    }
}

fn collect_nested_tables(module: &ModuleDescriptor) -> BTreeSet<Vec<&'static str>> {
    let mut tables = BTreeSet::new();
    for function in &module.functions {
        for index in 1..function.path.len() {
            tables.insert(function.path[..index].to_vec());
        }
    }
    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_nested_module_with_classes() {
        let class = ClassDescriptor {
            name: "Player",
            description: Some("Example userdata class."),
            fields: vec![FieldDescriptor {
                name: "name",
                ty: LuauType::literal("string"),
                description: Some("Player display name."),
            }],
            methods: vec![
                MethodDescriptor {
                    name: "new",
                    description: Some("Creates a player."),
                    params: vec![ParameterDescriptor {
                        name: "name",
                        ty: LuauType::literal("string"),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![LuauType::literal("Player")],
                    yields: false,
                    kind: MethodKind::Static,
                },
                MethodDescriptor {
                    name: "rename",
                    description: Some("Renames the player."),
                    params: vec![ParameterDescriptor {
                        name: "name",
                        ty: LuauType::literal("string"),
                        description: None,
                        variadic: false,
                    }],
                    returns: Vec::new(),
                    yields: false,
                    kind: MethodKind::Instance,
                },
            ],
        };
        let module = ModuleDescriptor {
            name: "Demo",
            local_name: "demo",
            description: Some("Prototype module."),
            functions: vec![
                ModuleFunctionDescriptor {
                    path: vec!["make_player"],
                    description: Some("Constructs a player."),
                    params: vec![ParameterDescriptor {
                        name: "name",
                        ty: LuauType::literal("string"),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![LuauType::literal("Player")],
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["util", "encode"],
                    description: Some("Encodes a value."),
                    params: vec![ParameterDescriptor {
                        name: "value",
                        ty: LuauType::literal("string"),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![LuauType::literal("string")],
                    yields: true,
                },
            ],
        };

        let rendered = render_definition_file(&module, &[class]).expect("render definition file");

        assert!(rendered.contains("@class Demo"));
        assert!(rendered.contains("demo.util = {}"));
        assert!(rendered.contains("function demo.util.encode(value: string): string"));
        assert!(rendered.contains("@interface Player"));
        assert!(rendered.contains("export type Player = {"));
        assert!(rendered.contains("rename: (self: Player, name: string) -> ()"));
        assert!(!rendered.contains("function Player.rename(self: Player, name: string)"));
        assert!(rendered.contains("function Player.new(name: string): Player"));
    }

    #[test]
    fn renders_optional_types_with_shorthand() {
        assert_eq!(
            LuauType::optional(LuauType::literal("number")).to_string(),
            "number?"
        );
        assert_eq!(
            LuauType::optional(LuauType::array(LuauType::literal("string"))).to_string(),
            "{string}?"
        );
        assert_eq!(
            LuauType::optional(LuauType::union(vec![
                LuauType::literal("string"),
                LuauType::literal("buffer"),
            ]))
            .to_string(),
            "(string | buffer)?"
        );
        assert_eq!(
            LuauType::optional(LuauType::optional(LuauType::literal("boolean"))).to_string(),
            "boolean?"
        );
    }

    #[test]
    fn renders_variadic_parameters_and_function_unions() {
        let callback = LuauType::function(
            vec![FunctionParameter {
                name: None,
                ty: LuauType::any(),
                variadic: true,
            }],
            Vec::new(),
        );

        assert_eq!(callback.to_string(), "(...any) -> ()");

        let signature = render_signature_params(&[ParameterDescriptor {
            name: "args",
            ty: LuauType::any(),
            description: None,
            variadic: true,
        }]);
        assert_eq!(signature, "...: any");

        assert_eq!(
            LuauType::union(vec![callback, LuauType::thread()]).to_string(),
            "((...any) -> ()) | thread"
        );
    }

    #[test]
    fn json_value_alias_descriptor_is_shared() {
        let descriptor = JsonValue::type_alias_descriptor();

        assert_eq!(descriptor.name, "JsonValue");
        assert_eq!(
            descriptor.ty.to_string(),
            "(boolean | number | string | {JsonValue} | { [string]: JsonValue })?"
        );
    }

    #[test]
    fn renders_variadic_global_with_doc_block() {
        let globals = vec![GlobalFunctionDescriptor {
            name: "warn",
            description: Some("Logs a warning."),
            params: vec![ParameterDescriptor {
                name: "values",
                ty: LuauType::any(),
                description: None,
                variadic: true,
            }],
            returns: Vec::new(),
            yields: false,
        }];
        let rendered = render_globals_definition(&globals).expect("render globals");
        assert!(rendered.contains("@function warn"));
        assert!(rendered.contains("Logs a warning."));
        assert!(rendered.contains("declare function warn(...: any)"));
        assert!(!rendered.contains("@yields"));
    }

    #[test]
    fn renders_multiple_globals_separated_by_blank_lines() {
        let globals = vec![
            GlobalFunctionDescriptor {
                name: "warn",
                description: None,
                params: vec![ParameterDescriptor {
                    name: "values",
                    ty: LuauType::any(),
                    description: None,
                    variadic: true,
                }],
                returns: Vec::new(),
                yields: false,
            },
            GlobalFunctionDescriptor {
                name: "print",
                description: None,
                params: vec![ParameterDescriptor {
                    name: "values",
                    ty: LuauType::any(),
                    description: None,
                    variadic: true,
                }],
                returns: Vec::new(),
                yields: true,
            },
        ];
        let rendered = render_globals_definition(&globals).expect("render globals");
        assert!(rendered.contains("declare function warn(...: any)"));
        assert!(rendered.contains("declare function print(...: any)"));
        assert!(rendered.contains("@yields"));
    }
}
