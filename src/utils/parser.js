"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseProductionCompanies = parseProductionCompanies;
exports.parseCastNames = parseCastNames;
exports.parseCast = parseCast;
const logger_1 = __importDefault(require("./logger"));
/**
 * 解析制作公司信息
 * 处理多种可能的格式：
 * 1. JSON字符串 - "[{\"name\":\"XXX\"},{\"name\":\"YYY\"}]"
 * 2. 逗号分隔的公司名 - "XXX, YYY, ZZZ"
 * 3. 管道符分隔的公司名 - "XXX|YYY|ZZZ"
 * 4. 方括号包围的单个公司名 - "[Boondocs]"
 */
function parseProductionCompanies(input) {
    if (!input)
        return [];
    try {
        // 特殊处理方括号包围的单个公司名(如[Boondocs])
        if (typeof input === 'string' && input.startsWith('[') && input.endsWith(']')) {
            // 检查是否是简单的[CompanyName]格式
            const simpleCompanyRegex = /^\[([\w\s&.-]+)\]$/;
            const match = input.match(simpleCompanyRegex);
            if (match) {
                // 直接提取公司名
                return [{ name: match[1].trim() }];
            }
            // 否则尝试解析为JSON
            try {
                const parsed = JSON.parse(input);
                if (Array.isArray(parsed)) {
                    return parsed.map((c) => {
                        if (typeof c === 'string')
                            return { name: c };
                        return { name: c.name || 'Unknown', id: c.id || null };
                    });
                }
            }
            catch (jsonError) {
                // JSON解析失败，尝试其他方式
                logger_1.default.debug(`[Boondocs]格式公司名解析: ${input}`);
            }
        }
        // 检查是否已经是JSON对象（或数组对象）
        if (typeof input === 'object' && input !== null) {
            // 显式断言为数组类型，以解决TypeScript类型推断问题
            if (Array.isArray(input)) {
                const arrayInput = input;
                return arrayInput.map((c) => {
                    if (typeof c === 'string')
                        return { name: c };
                    return { name: c.name || 'Unknown', id: c.id || null };
                });
            }
            // 单个对象
            return [{ name: input.name || 'Unknown', id: input.id || null }];
        }
        // 此时input必定是字符串
        const strInput = input;
        // 尝试使用管道符分隔
        if (strInput.includes('|')) {
            return strInput.split('|').map(name => ({ name: name.trim() }));
        }
        // 使用逗号分隔（最常见的情况）
        return strInput.split(/,\s*/).filter(Boolean).map(name => ({ name: name.trim() }));
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger_1.default.warn(`解析制作公司失败，使用备用解析: ${errorMessage}`, { input });
        // 备用解析：尝试各种常见分隔符
        try {
            if (typeof input !== 'string')
                return [{ name: String(input) }];
            // 检查是否是[CompanyName]格式
            if (input.startsWith('[') && input.endsWith(']')) {
                // 简单提取方括号中的内容
                const companyName = input.substring(1, input.length - 1).trim();
                return [{ name: companyName }];
            }
            if (input.includes(',')) {
                return input.split(/,\s*/).filter(Boolean).map(name => ({ name: name.trim() }));
            }
            else if (input.includes('|')) {
                return input.split('|').map(name => ({ name: name.trim() }));
            }
            else if (input.includes(';')) {
                return input.split(';').map(name => ({ name: name.trim() }));
            }
            else {
                // 单个公司
                return [{ name: input.trim() }];
            }
        }
        catch (e) {
            const eMessage = e instanceof Error ? e.message : String(e);
            logger_1.default.error(`备用解析制作公司也失败: ${eMessage}`);
            return [{ name: String(input) }]; // 最后的容错措施
        }
    }
}
/**
 * 解析演员信息为名称数组
 * 处理多种可能的格式：
 * 1. JSON字符串数组 - "[\"Actor1\",\"Actor2\"]"
 * 2. 逗号分隔的演员名 - "Actor1, Actor2, Actor3"
 * 3. 带角色的文本表示 - "Actor1 as Role1, Actor2 as Role2"
 *
 * 返回格式：演员名称数组 ["Actor1", "Actor2", ...]
 */
function parseCastNames(input) {
    if (!input)
        return [];
    try {
        // 尝试解析JSON
        if (typeof input === 'string' && input.startsWith('[') && input.endsWith(']')) {
            try {
                const parsed = JSON.parse(input);
                if (Array.isArray(parsed)) {
                    return parsed.map((actor) => {
                        if (typeof actor === 'string')
                            return actor;
                        if (typeof actor === 'object' && actor !== null) {
                            return actor.name || 'Unknown Actor';
                        }
                        return String(actor);
                    });
                }
            }
            catch (e) {
                const eMessage = e instanceof Error ? e.message : String(e);
                logger_1.default.warn(`无法解析演员JSON: ${eMessage}`);
                // 继续尝试其他格式
            }
        }
        // 如果已经是数组
        if (Array.isArray(input)) {
            return input.map((actor) => {
                if (typeof actor === 'string')
                    return actor;
                if (typeof actor === 'object' && actor !== null) {
                    return actor.name || 'Unknown Actor';
                }
                return String(actor);
            });
        }
        // 此时input必定是字符串
        const strInput = input;
        // 按 "name as character" 格式解析，但只返回名称部分
        if (strInput.includes(' as ')) {
            return strInput.split(/(?<=[^\s]),\s*(?=[A-Z])/g).map(entry => {
                const [namePart] = entry.split(/ as /i);
                return namePart.trim();
            });
        }
        // 简单的逗号分隔列表
        return strInput.split(/,\s*/).filter(Boolean).map(name => name.trim());
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger_1.default.warn(`解析演员信息失败，使用备用解析: ${errorMessage}`, { input });
        // 最后的容错处理
        try {
            if (typeof input !== 'string')
                return [];
            return input.split(/,\s*/).filter(Boolean).map(name => name.trim());
        }
        catch (e) {
            const eMessage = e instanceof Error ? e.message : String(e);
            logger_1.default.error(`备用解析演员也失败: ${eMessage}`);
            return []; // 实在不行，返回空数组
        }
    }
}
/**
 * 解析演员信息为结构化Json对象数组
 * 处理多种可能的格式：
 * 1. JSON字符串 - "[{\"name\":\"XXX\",\"character\":\"YYY\"}]"
 * 2. 逗号分隔的演员名 - "Actor1, Actor2, Actor3"
 * 3. 带角色的文本表示 - "Actor1 as Role1, Actor2 as Role2"
 *
 * 返回格式：[{name: "Actor1"}, {name: "Actor2"}, ...]
 */
function parseCast(input) {
    if (!input)
        return [];
    try {
        // 尝试解析JSON
        if (typeof input === 'string' && input.startsWith('[') && input.endsWith(']')) {
            try {
                const parsed = JSON.parse(input);
                if (Array.isArray(parsed)) {
                    return parsed.map((actor) => {
                        if (typeof actor === 'string')
                            return { name: actor };
                        if (typeof actor === 'object' && actor !== null) {
                            return { name: actor.name || 'Unknown Actor' };
                        }
                        return { name: String(actor) };
                    });
                }
            }
            catch (e) {
                // 继续尝试其他格式
            }
        }
        // 如果已经是数组
        if (Array.isArray(input)) {
            return input.map((actor) => {
                if (typeof actor === 'string')
                    return { name: actor };
                if (typeof actor === 'object' && actor !== null) {
                    return { name: actor.name || 'Unknown Actor' };
                }
                return { name: String(actor) };
            });
        }
        // 此时input必定是字符串
        const strInput = input;
        // 按 "name as character" 格式解析
        if (strInput.includes(' as ')) {
            return strInput.split(/(?<=[^\s]),\s*(?=[A-Z])/g).map(entry => {
                const [namePart] = entry.split(/ as /i);
                return { name: namePart.trim() };
            });
        }
        // 简单的逗号分隔列表
        return strInput.split(/,\s*/).filter(Boolean).map(name => ({
            name: name.trim()
        }));
    }
    catch (error) {
        // 最后的容错处理
        try {
            if (typeof input !== 'string')
                return [];
            return input.split(/,\s*/).filter(Boolean).map(name => ({
                name: name.trim()
            }));
        }
        catch (e) {
            return []; // 实在不行，返回空数组
        }
    }
}
