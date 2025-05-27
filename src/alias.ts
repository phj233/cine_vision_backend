import moduleAlias from 'module-alias';
import path from 'path';

// 注册别名
moduleAlias.addAliases({
    '@': path.join(__dirname)
}); 