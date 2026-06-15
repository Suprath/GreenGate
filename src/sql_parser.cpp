#include "green_gate/sql_parser.hpp"
#include <stdexcept>
#include <cctype>
#include <algorithm>

namespace greengate {

std::vector<SqlParser::Token> SqlParser::Tokenize(const std::string& src) {
    std::vector<Token> tokens;
    size_t i = 0;
    while (i < src.length()) {
        char c = src[i];
        
        // Skip spaces
        if (std::isspace(c)) {
            i++;
            continue;
        }
        
        // Parentheses
        if (c == '(') {
            tokens.push_back({Token::Type::LPAREN, "("});
            i++;
            continue;
        }
        if (c == ')') {
            tokens.push_back({Token::Type::RPAREN, ")"});
            i++;
            continue;
        }
        
        // Operators
        if (c == '>') {
            if (i + 1 < src.length() && src[i + 1] == '=') {
                tokens.push_back({Token::Type::GE, ">="});
                i += 2;
            } else {
                tokens.push_back({Token::Type::GT, ">"});
                i++;
            }
            continue;
        }
        if (c == '<') {
            if (i + 1 < src.length() && src[i + 1] == '=') {
                tokens.push_back({Token::Type::LE, "<="});
                i += 2;
            } else {
                tokens.push_back({Token::Type::LT, "<"});
                i++;
            }
            continue;
        }
        if (c == '=') {
            tokens.push_back({Token::Type::EQ, "="});
            i++;
            continue;
        }
        
        // Numbers
        if (std::isdigit(c)) {
            std::string num;
            while (i < src.length() && std::isdigit(src[i])) {
                num += src[i];
                i++;
            }
            tokens.push_back({Token::Type::NUMBER, num});
            continue;
        }
        
        // Identifiers / Logical Keywords
        if (std::isalpha(c) || c == '_') {
            std::string word;
            while (i < src.length() && (std::isalnum(src[i]) || src[i] == '_')) {
                word += src[i];
                i++;
            }
            
            // Convert to uppercase for keyword comparison
            std::string upper_word = word;
            std::transform(upper_word.begin(), upper_word.end(), upper_word.begin(), ::toupper);
            
            if (upper_word == "AND") {
                tokens.push_back({Token::Type::AND, "AND"});
            } else {
                tokens.push_back({Token::Type::IDENTIFIER, word});
            }
            continue;
        }
        
        throw std::runtime_error(std::string("SQL Tokenizer Error: Unexpected character: ") + c);
    }
    
    tokens.push_back({Token::Type::END, ""});
    return tokens;
}

apex::ir::Node* SqlParser::Parse(const std::string& sql_predicate) {
    auto tokens = Tokenize(sql_predicate);
    size_t pos = 0;
    apex::ir::Node* root = ParseExpression(tokens, pos);
    
    if (tokens[pos].type != Token::Type::END) {
        throw std::runtime_error("SQL Parser Error: Trailing tokens at position " + std::to_string(pos));
    }
    return root;
}

// Expression ::= Term { "AND" Term }
apex::ir::Node* SqlParser::ParseExpression(const std::vector<Token>& tokens, size_t& pos) {
    apex::ir::Node* node = ParseTerm(tokens, pos);
    
    while (tokens[pos].type == Token::Type::AND) {
        pos++; // consume "AND"
        apex::ir::Node* right = ParseTerm(tokens, pos);
        node = apex::builder::And(node, right);
    }
    
    return node;
}

// Term ::= Primary [ (">" | "<" | ">=" | "<=" | "=") Primary ]
apex::ir::Node* SqlParser::ParseTerm(const std::vector<Token>& tokens, size_t& pos) {
    apex::ir::Node* left = ParsePrimary(tokens, pos);
    
    Token::Type type = tokens[pos].type;
    if (type == Token::Type::GT || type == Token::Type::LT || 
        type == Token::Type::GE || type == Token::Type::LE || 
        type == Token::Type::EQ) {
        
        pos++; // consume comparison operator
        apex::ir::Node* right = ParsePrimary(tokens, pos);
        
        switch (type) {
            case Token::Type::GT: return apex::builder::GT(left, right);
            case Token::Type::LT: return apex::builder::LT(left, right);
            case Token::Type::GE: return apex::builder::GE(left, right);
            case Token::Type::LE: return apex::builder::LE(left, right);
            case Token::Type::EQ: return apex::builder::EQ(left, right);
            default: break;
        }
    }
    
    return left;
}

// Primary ::= Identifier | Number | "(" Expression ")"
apex::ir::Node* SqlParser::ParsePrimary(const std::vector<Token>& tokens, size_t& pos) {
    if (pos >= tokens.size()) {
        throw std::runtime_error("SQL Parser Error: Unexpected end of input");
    }
    
    if (tokens[pos].type == Token::Type::LPAREN) {
        pos++; // consume "("
        apex::ir::Node* node = ParseExpression(tokens, pos);
        
        if (tokens[pos].type != Token::Type::RPAREN) {
            throw std::runtime_error("SQL Parser Error: Mismatched parentheses, expected ')'");
        }
        pos++; // consume ")"
        return node;
    }
    
    if (tokens[pos].type == Token::Type::IDENTIFIER) {
        apex::ir::Node* node = apex::builder::Load(tokens[pos].text.c_str());
        pos++;
        return node;
    }
    
    if (tokens[pos].type == Token::Type::NUMBER) {
        int64_t val = std::stoll(tokens[pos].text);
        apex::ir::Node* node = apex::builder::Const(val);
        pos++;
        return node;
    }
    
    throw std::runtime_error("SQL Parser Error: Unexpected token: " + tokens[pos].text);
}

} // namespace greengate
