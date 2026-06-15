#pragma once

#include "apex/jit/ir.hpp"
#include <string>
#include <vector>

namespace greengate {

class SqlParser {
public:
    SqlParser() = default;

    // Parse a SQL predicate string and return AarchGate JIT AST Node
    apex::ir::Node* Parse(const std::string& sql_predicate);

private:
    struct Token {
        enum class Type { 
            IDENTIFIER, 
            NUMBER, 
            AND, 
            GT, 
            LT, 
            GE, 
            LE, 
            EQ, 
            LPAREN, 
            RPAREN, 
            END 
        };
        Type type;
        std::string text;
    };

    std::vector<Token> Tokenize(const std::string& src);
    
    // Recursive Descent parsing methods
    apex::ir::Node* ParseExpression(const std::vector<Token>& tokens, size_t& pos);
    apex::ir::Node* ParseTerm(const std::vector<Token>& tokens, size_t& pos);
    apex::ir::Node* ParsePrimary(const std::vector<Token>& tokens, size_t& pos);
};

} // namespace greengate
